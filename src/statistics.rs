use std::io::Write;
use std::io;
use crate::Serializable;

pub trait Statistics: Serializable {
    fn to_struct_i32(&self) -> StatisticsStruct<i32>;
}

#[derive(Copy, Clone)]
pub struct StatisticsStruct<T> {
    ts_first: i64,
    ts_last: i64,

    min_value: T,
    max_value: T,
    first_value: T,
    last_value: T,
    sum_value: i64,
}

impl StatisticsStruct<i32> {
    pub(crate) fn new() -> StatisticsStruct<i32> {
        StatisticsStruct {
            ts_first: i64::MAX,
            ts_last: i64::MIN,
            min_value: i32::MAX,
            max_value: i32::MIN,
            first_value: 0,
            last_value: 0,
            sum_value: 0,
        }
    }

    pub(crate) fn merge(&mut self, statistics: &StatisticsStruct<i32>) {
        if statistics.ts_first < self.ts_first {
            self.ts_first = statistics.ts_first;
            self.first_value = statistics.first_value;
        }
        if statistics.ts_last > self.ts_last {
            self.ts_last = statistics.ts_first;
            self.last_value = statistics.last_value;
        }
        if statistics.max_value > self.max_value {
            self.max_value = statistics.max_value;
        }
        if statistics.min_value < self.min_value {
            self.min_value = statistics.min_value;
        }
        self.sum_value = self.sum_value + statistics.sum_value;
    }

    pub(crate) fn update(&mut self, timestamp: i64, value: i32) {
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
        self.sum_value += value as i64;
    }
}

impl Serializable for StatisticsStruct<i32> {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
        file.write_all(&self.min_value.to_be_bytes());
        file.write_all(&self.max_value.to_be_bytes());
        file.write_all(&self.first_value.to_be_bytes());
        file.write_all(&self.last_value.to_be_bytes());
        file.write_all(&self.sum_value.to_be_bytes())
    }
}

impl Statistics for StatisticsStruct<i32> {
    fn to_struct_i32(&self) -> StatisticsStruct<i32> {
        self.clone()
    }
}

#[derive(Copy, Clone)]
struct LongStatistics {
    ts_first: i64,
    ts_last: i64,

    min_value: i64,
    max_value: i64,
    first_value: i64,
    last_value: i64,
    sum_value: i64,
}

impl LongStatistics {
    fn new() -> LongStatistics {
        LongStatistics {
            ts_first: i64::MAX,
            ts_last: i64::MIN,
            min_value: i64::MAX,
            max_value: i64::MIN,
            first_value: 0,
            last_value: 0,
            sum_value: 0,
        }
    }

    fn update(&mut self, timestamp: i64, value: i64) {
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
        self.sum_value += value;
    }
}

impl Serializable for LongStatistics {
    fn serialize(&self, file: &mut dyn Write) -> io::Result<()> {
        file.write_all(&self.min_value.to_be_bytes());
        file.write_all(&self.max_value.to_be_bytes());
        file.write_all(&self.first_value.to_be_bytes());
        file.write_all(&self.last_value.to_be_bytes());
        file.write_all(&self.sum_value.to_be_bytes())
    }
}

impl Statistics for LongStatistics {
    fn to_struct_i32(&self) -> StatisticsStruct<i32> {
        // this should not work!
        todo!()
    }
}
