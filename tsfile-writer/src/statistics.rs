use crate::utils::size_var_u32;
use crate::{write_var_u32, IoTDBValue, PositionedWrite, Serializable, TSDataType, TsFileError};

#[derive(Clone, Debug)]
pub enum Statistics {
    INT32(StatisticsStruct<i32, i64>),
    INT64(StatisticsStruct<i64, f64>),
    FLOAT(StatisticsStruct<f32, f64>),
}

impl Statistics {
    pub(crate) fn count(&self) -> u32 {
        match self {
            Statistics::INT32(s) => s.count,
            Statistics::INT64(s) => s.count,
            Statistics::FLOAT(s) => s.count,
        }
    }
    pub(crate) fn get_serialized_size(&self) -> u32 {
        match self {
            Statistics::INT32(s) => s.get_serialized_size(),
            Statistics::INT64(s) => s.get_serialized_size(),
            Statistics::FLOAT(s) => s.get_serialized_size(),
        }
    }
}

impl Statistics {
    pub(crate) fn update(&mut self, timestamp: i64, value: &IoTDBValue) {
        match (self, value) {
            (Statistics::INT32(s), IoTDBValue::INT(v)) => s.update(timestamp, *v),
            (Statistics::INT64(s), IoTDBValue::LONG(v)) => s.update(timestamp, *v),
            (Statistics::FLOAT(s), IoTDBValue::FLOAT(v)) => s.update(timestamp, *v),
            _ => todo!(),
        }
    }
}

impl Statistics {
    pub(crate) fn merge(&mut self, other: &Statistics) {
        match self {
            Statistics::INT32(s) => match other {
                Statistics::INT32(othr) => s.merge(othr),
                _ => {
                    panic!("...")
                }
            },
            Statistics::INT64(s) => match other {
                Statistics::INT64(othr) => s.merge(othr),
                _ => {
                    panic!("...")
                }
            },
            Statistics::FLOAT(s) => match other {
                Statistics::FLOAT(othr) => s.merge(othr),
                _ => {
                    panic!("...")
                }
            },
        }
    }
}

impl Statistics {
    pub fn new(data_type: TSDataType) -> Statistics {
        match data_type {
            TSDataType::INT32 => Statistics::INT32(StatisticsStruct::<i32, i64>::new()),
            TSDataType::INT64 => Statistics::INT64(StatisticsStruct::<i64, f64>::new()),
            TSDataType::FLOAT => Statistics::FLOAT(StatisticsStruct::<f32, f64>::new()),
        }
    }
}

impl Serializable for Statistics {
    fn serialize(&self, file: &mut dyn PositionedWrite) -> Result<(), TsFileError> {
        match self {
            Statistics::INT32(s) => s.serialize(file),
            Statistics::INT64(s) => s.serialize(file),
            Statistics::FLOAT(s) => s.serialize(file),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct StatisticsStruct<T, S> {
    ts_first: i64,
    ts_last: i64,

    min_value: T,
    max_value: T,
    first_value: T,
    last_value: T,
    count: u32,
    sum_value: S,
}

impl StatisticsStruct<i64, f64> {
    pub(crate) fn get_serialized_size(&self) -> u32 {
        // return ReadWriteForEncodingUtils.uVarIntSize(count) // count
        // + 16 // startTime, endTime
        // + getStatsSize();
        // 40 -> stat size for int
        size_var_u32(self.count) as u32 + 16 + 40
    }
}

impl StatisticsStruct<i32, i64> {
    pub(crate) fn get_serialized_size(&self) -> u32 {
        size_var_u32(self.count) as u32 + 16 + 24
    }
}

impl StatisticsStruct<f32, f64> {
    pub(crate) fn get_serialized_size(&self) -> u32 {
        size_var_u32(self.count) as u32 + 16 + 24
    }
}

#[macro_export]
macro_rules! implement_statistics {
    ( $type:ty, $sum:ty ) => {
        impl StatisticsStruct<$type, $sum> {
            pub(crate) fn new() -> StatisticsStruct<$type, $sum> {
                StatisticsStruct {
                    ts_first: i64::MAX,
                    ts_last: i64::MIN,
                    min_value: <$type>::MAX,
                    max_value: <$type>::MIN,
                    first_value: 0 as $type,
                    last_value: 0 as $type,
                    count: 0,
                    sum_value: 0 as $sum,
                }
            }

            pub(crate) fn merge(&mut self, statistics: &StatisticsStruct<$type, $sum>) {
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
                self.count += statistics.count;
                self.sum_value += statistics.sum_value;
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
                self.sum_value += value as $sum;
            }
        }

        impl Serializable for StatisticsStruct<$type, $sum> {
            fn serialize(&self, file: &mut dyn PositionedWrite) -> Result<(), TsFileError> {
                // Header for statistics
                write_var_u32(self.count as u32, file)?;
                file.write_all(&self.ts_first.to_be_bytes())?;
                file.write_all(&self.ts_last.to_be_bytes())?;

                file.write_all(&self.min_value.to_be_bytes())?;
                file.write_all(&self.max_value.to_be_bytes())?;
                file.write_all(&self.first_value.to_be_bytes())?;
                file.write_all(&self.last_value.to_be_bytes())?;
                file.write_all(&self.sum_value.to_be_bytes())?;

                Ok(())
            }
        }
    };
}

implement_statistics!(i32, i64);
implement_statistics!(i64, f64);
implement_statistics!(f32, f64);
// TODO Implement / use
// implement_statistics!(f64);
