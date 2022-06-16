#[derive(Clone, Copy)]
pub struct TsFileConfig {
    pub(crate) max_degree_of_index_node: usize,
    pub(crate) bloom_filter_error_rate: f64,
    pub(crate) min_bloom_filter_error_rate: f64,
    pub(crate) max_bloom_filter_error_rate: f64,
    pub(crate) minimal_size: i32,
    pub(crate) maximal_hash_function_size: i32,
    pub(crate) seeds: [u8; 8],
}

impl Default for TsFileConfig {
    fn default() -> Self {
        Self {
            max_degree_of_index_node: 256,
            bloom_filter_error_rate: 0.05,
            min_bloom_filter_error_rate: 0.01,
            max_bloom_filter_error_rate: 0.1,
            minimal_size: 256,
            maximal_hash_function_size: 8,
            seeds: [5, 7, 11, 19, 31, 37, 43, 59]
        }
    }
}
