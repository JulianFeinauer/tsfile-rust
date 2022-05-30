use std::io;

use crate::{PositionedWrite, Serializable, write_var_u32};

pub trait Statistics<T>: Serializable {}

#[derive(Copy, Clone)]
pub struct StatisticsStruct<T> {
    ts_first: i64,
    ts_last: i64,

    min_value: T,
    max_value: T,
    first_value: T,
    last_value: T,
    count: u64,
    sum_value: i64,
}

#[macro_export]
macro_rules! implement_statistics {
    ( $type:ty ) => {
            impl StatisticsStruct<$type> {
                pub(crate) fn new() -> StatisticsStruct<$type> {
                    StatisticsStruct {
                        ts_first: i64::MAX,
                        ts_last: i64::MIN,
                        min_value: <$type>::MAX,
                        max_value: <$type>::MIN,
                        first_value: 0 as $type,
                        last_value: 0 as $type,
                        count: 0,
                        sum_value: 0,
                    }
                }

                pub(crate) fn merge(&mut self, statistics: &StatisticsStruct<$type>) {
                    if statistics.ts_first < self.ts_first {
                        self.ts_first = statistics.ts_first;
                        self.first_value = statistics.first_value;
                    }
                    if statistics.ts_last > self.ts_last {
                        self.ts_last = statistics.ts_last;
                        self.last_value = statistics.last_value;
                    }
                    if statistics.max_value > self.max_value {
                        self.max_value = statistics.max_value;
                    }
                    if statistics.min_value < self.min_value {
                        self.min_value = statistics.min_value;
                    }
                    self.count = self.count + statistics.count;
                    self.sum_value = self.sum_value + statistics.sum_value;
                }

                pub(crate) fn update(&mut self, timestamp: i64, value: $type) {
                    if timestamp < self.ts_first {
                        self.ts_first = timestamp;
                        self.first_value = value;
                    }
                    if timestamp > self.ts_last {
                        self.ts_last = timestamp;
                        self.last_value = value;
                    }
                    if value < self.min_value {
                        self.min_value = value;
                    }
                    if value > self.max_value {
                        self.max_value = value;
                    }
                    self.count += 1;
                    self.sum_value += value as i64;
                }

            }


            impl Serializable for StatisticsStruct<$type> {
                fn serialize(&self, file: &mut dyn PositionedWrite) -> io::Result<()> {
                    // Header for statistics
                    write_var_u32(self.count as u32, file);
                    file.write_all(&self.ts_first.to_be_bytes());
                    file.write_all(&self.ts_last.to_be_bytes());

                    file.write_all(&self.min_value.to_be_bytes());
                    file.write_all(&self.max_value.to_be_bytes());
                    file.write_all(&self.first_value.to_be_bytes());
                    file.write_all(&self.last_value.to_be_bytes());
                    file.write_all(&self.sum_value.to_be_bytes())
                }
            }

            impl Statistics<$type> for StatisticsStruct<$type> {}
        }
    }


#[macro_export]
macro_rules! implement_int_statistics {
    ( $type:ty ) => {
        implement_statistics!($type);
    }
    }

implement_int_statistics!(i32);
implement_statistics!(i64);
// TODO Implement / use
// implement_statistics!(f32);
// implement_statistics!(f64);
