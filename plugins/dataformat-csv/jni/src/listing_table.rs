use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use std::any::Any;
use std::result;
use datafusion::common::{project_schema, Constraints, Statistics};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::catalog::Session;
use datafusion::logical_expr::Expr;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile};
use datafusion::datasource::listing::helpers::{expr_applicable_for_cols, pruned_partition_list};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::{create_ordering};
use datafusion::error::DataFusionError;
use datafusion::execution::cache::cache_manager::FileStatisticsCache;
use datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::schema_rewriter::PhysicalExprAdapterFactory;
use datafusion::physical_plan::empty::EmptyExec;
use futures::Stream;
use futures_util::{future, stream, StreamExt};
use object_store::ObjectStore;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct ShardListingTable {
    inner: ListingTable,
    collected_statistics: FileStatisticsCache,
}

impl ShardListingTable {
    pub fn new(listing_table: ListingTable) -> Self {
        Self {
            inner: listing_table,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
        }
    }

    pub fn try_new(config: ListingTableConfig) -> Result<Self> {
        Ok(Self {
            inner: ListingTable::try_new(config)?,
            collected_statistics: Arc::new(DefaultFileStatisticsCache::default()),
        })
    }

        // Expose any ListingTable methods you want to make available directly
    pub fn table_paths(&self) -> &Vec<ListingTableUrl> {
        self.inner.table_paths()
    }

    pub fn options(&self) -> &ListingOptions {
        self.inner.options()
    }

    async fn list_files_for_scan<'a>(
        &'a self,
        ctx: &'a dyn Session,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> Result<(Vec<FileGroup>, Statistics), DataFusionError> {
        let store = if let Some(url) = self.inner.table_paths().first() {
            ctx.runtime_env().object_store(url)?
        } else {
            return Ok((vec![], Statistics::new_unknown(&self.inner.schema())));
        };
        // list files (with partitions)
        let file_list = future::try_join_all(self.table_paths().iter().map(|table_path| {
            pruned_partition_list(
                ctx,
                store.as_ref(),
                table_path,
                filters,
                &self.options().file_extension,
                &self.options().table_partition_cols,
            )
        }))
            .await?;
        let meta_fetch_concurrency =
            ctx.config_options().execution.meta_fetch_concurrency;
        let file_list = stream::iter(file_list).flatten_unordered(meta_fetch_concurrency);
        // collect the statistics if required by the config
        let files = file_list
            .map(|part_file| async {
                let part_file = part_file?;
                let statistics = if self.options().collect_stat {
                    self.do_collect_statistics(ctx, &store, &part_file).await?
                } else {
                    Arc::new(Statistics::new_unknown(&self.inner.schema()))
                };
                Ok(part_file.with_statistics(statistics))
            })
            .boxed()
            .buffer_unordered(ctx.config_options().execution.meta_fetch_concurrency);

        let (file_group, inexact_stats) =
            get_files_with_limit(files, limit, self.options().collect_stat).await?;

        let file_groups = file_group.split_files(self.options().target_partitions);
        compute_all_files_statistics(
            file_groups,
            self.schema(),
            self.options().collect_stat,
            inexact_stats,
        )
    }

    /// Collects statistics for a given partitioned file.
    ///
    /// This method first checks if the statistics for the given file are already cached.
    /// If they are, it returns the cached statistics.
    /// If they are not, it infers the statistics from the file and stores them in the cache.
    async fn do_collect_statistics(
        &self,
        ctx: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        part_file: &PartitionedFile,
    ) -> Result<Arc<Statistics>> {
        match self
            .collected_statistics
            .get_with_extra(&part_file.object_meta.location, &part_file.object_meta)
        {
            Some(statistics) => Ok(statistics),
            None => {
                let statistics = self
                    .options()
                    .format
                    .infer_stats(
                        ctx,
                        store,
                        Arc::clone(&self.schema()),
                        &part_file.object_meta,
                    )
                    .await?;
                let statistics = Arc::new(statistics);
                self.collected_statistics.put_with_extra(
                    &part_file.object_meta.location,
                    Arc::clone(&statistics),
                    &part_file.object_meta,
                );
                Ok(statistics)
            }
        }
    }

    /// If file_sort_order is specified, creates the appropriate physical expressions
    fn try_create_output_ordering(&self) -> datafusion::common::Result<Vec<LexOrdering>> {
        create_ordering(&self.schema(), &self.options().file_sort_order)
    }
}

async fn get_files_with_limit(
    files: impl Stream<Item = Result<PartitionedFile>>,
    limit: Option<usize>,
    collect_stats: bool,
) -> Result<(FileGroup, bool)> {
    let mut file_group = FileGroup::default();
    // Fusing the stream allows us to call next safely even once it is finished.
    let mut all_files = Box::pin(files.fuse());
    enum ProcessingState {
        ReadingFiles,
        ReachedLimit,
    }

    let mut state = ProcessingState::ReadingFiles;
    let mut num_rows = Precision::Absent;

    while let Some(file_result) = all_files.next().await {
        // Early exit if we've already reached our limit
        if matches!(state, ProcessingState::ReachedLimit) {
            break;
        }

        let file = file_result?;

        // Update file statistics regardless of state
        if collect_stats {
            if let Some(file_stats) = &file.statistics {
                num_rows = if file_group.is_empty() {
                    // For the first file, just take its row count
                    file_stats.num_rows
                } else {
                    // For subsequent files, accumulate the counts
                    num_rows.add(&file_stats.num_rows)
                };
            }
        }

        // Always add the file to our group
        file_group.push(file);

        // Check if we've hit the limit (if one was specified)
        if let Some(limit) = limit {
            if let Precision::Exact(row_count) = num_rows {
                if row_count > limit {
                    state = ProcessingState::ReachedLimit;
                }
            }
        }
    }
    // If we still have files in the stream, it means that the limit kicked
    // in, and the statistic could have been different had we processed the
    // files in a different order.
    let inexact_stats = all_files.next().await.is_some();
    Ok((file_group, inexact_stats))
}

#[async_trait]
impl TableProvider for ShardListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    // Override just the scan method with your custom implementation
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // extract types of partition columns
        let table_partition_cols = self
            .options()
            .table_partition_cols
            .iter()
            .map(|col| Ok(self.schema().field_with_name(&col.0)?.clone()))
            .collect::<Result<Vec<_>>>()?;

        let table_partition_col_names = table_partition_cols
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        // If the filters can be resolved using only partition cols, there is no need to
        // pushdown it to TableScan, otherwise, `unhandled` pruning predicates will be generated
        let (partition_filters, filters): (Vec<_>, Vec<_>) =
            filters.iter().cloned().partition(|filter| {
                can_be_evaluted_for_partition_pruning(&table_partition_col_names, filter)
            });

        // We should not limit the number of partitioned files to scan if there are filters and limit
        // at the same time. This is because the limit should be applied after the filters are applied.
        let statistic_file_limit = if filters.is_empty() { limit } else { None };

        let (mut partitioned_file_lists, statistics) = self
            .list_files_for_scan(state, &partition_filters, statistic_file_limit)
            .await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let projected_schema = project_schema(&self.schema(), projection)?;
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let output_ordering = self.try_create_output_ordering()?;
        match state
            .config_options()
            .execution
            .split_file_groups_by_statistics
            .then(|| {
                output_ordering.first().map(|output_ordering| {
                    FileScanConfig::split_groups_by_statistics_with_target_partitions(
                        &self.schema(),
                        &partitioned_file_lists,
                        output_ordering,
                        self.options().target_partitions,
                    )
                })
            })
            .flatten()
        {
            Some(Err(e)) => log::debug!("failed to split file groups by statistics: {e}"),
            Some(Ok(new_groups)) => {
                if new_groups.len() <= self.options().target_partitions {
                    partitioned_file_lists = new_groups;
                } else {
                    log::debug!("attempted to split file groups by statistics, but there were more file groups than target_partitions; falling back to unordered")
                }
            }
            None => {} // no ordering required
        };

        let Some(object_store_url) =
            self.table_paths().first().map(ListingTableUrl::object_store)
        else {
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))));
        };

        // let file_source = self.create_file_source_with_schema_adapter()?;

        // create the execution plan
        self.options()
            .format
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(
                    object_store_url,
                    Arc::clone(&self.schema()),
                    self.options().format.file_source(),
                )
                    .with_file_groups(partitioned_file_lists)
                    .with_constraints(self.constraints().unwrap().clone())
                    .with_statistics(statistics)
                    .with_projection(projection.cloned())
                    .with_limit(limit)
                    .with_output_ordering(output_ordering)
                    .with_table_partition_cols(table_partition_cols)
                    .with_expr_adapter(None)
                    .build(),
            )
            .await
    }


    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.inner.insert_into(state, input, insert_op).await
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }
}

// Expressions can be used for parttion pruning if they can be evaluated using
// only the partiton columns and there are partition columns.
fn can_be_evaluted_for_partition_pruning(
    partition_column_names: &[&str],
    expr: &Expr,
) -> bool {
    !partition_column_names.is_empty()
        && expr_applicable_for_cols(partition_column_names, expr)
}

pub fn compute_file_group_statistics(
    file_group: FileGroup,
    file_schema: SchemaRef,
    collect_stats: bool,
) -> datafusion::common::Result<FileGroup> {
    if !collect_stats {
        return Ok(file_group);
    }

    let file_group_stats = file_group.iter().filter_map(|file| {
        let stats = file.statistics.as_ref()?;
        Some(stats.as_ref())
    });
    let statistics = Statistics::try_merge_iter(file_group_stats, &file_schema)?;

    Ok(file_group.with_statistics(Arc::new(statistics)))
}

/// Computes statistics for all files across multiple file groups.
///
/// This function:
/// 1. Computes statistics for each individual file group
/// 2. Summary statistics across all file groups
/// 3. Optionally marks statistics as inexact
///
/// # Parameters
/// * `file_groups` - Vector of file groups to process
/// * `table_schema` - Schema of the table
/// * `collect_stats` - Whether to collect statistics
/// * `inexact_stats` - Whether to mark the resulting statistics as inexact
///
/// # Returns
/// A tuple containing:
/// * The processed file groups with their individual statistics attached
/// * The summary statistics across all file groups, aka all files summary statistics
pub fn compute_all_files_statistics(
    file_groups: Vec<FileGroup>,
    table_schema: SchemaRef,
    collect_stats: bool,
    inexact_stats: bool,
) -> datafusion::common::Result<(Vec<FileGroup>, Statistics)> {
    let file_groups_with_stats = file_groups
        .into_iter()
        .map(|file_group| {
            compute_file_group_statistics(
                file_group,
                Arc::clone(&table_schema),
                collect_stats,
            )
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?;

    // Then summary statistics across all file groups
    let file_groups_statistics = file_groups_with_stats
        .iter()
        .filter_map(|file_group| file_group.file_statistics(None));

    let mut statistics =
        Statistics::try_merge_iter(file_groups_statistics, &table_schema)?;

    if inexact_stats {
        statistics = statistics.to_inexact()
    }

    Ok((file_groups_with_stats, statistics))
}