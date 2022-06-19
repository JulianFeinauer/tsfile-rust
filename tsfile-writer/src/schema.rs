use crate::{CompressionType, MeasurementGroup, MeasurementSchema, Schema, TSDataType, TSEncoding};
use std::collections::HashMap;

pub struct TsFileSchemaBuilder {
    measurement_groups_map: HashMap<String, MeasurementGroup>,
}

impl TsFileSchemaBuilder {
    pub fn new() -> TsFileSchemaBuilder {
        TsFileSchemaBuilder {
            measurement_groups_map: HashMap::new(),
        }
    }

    pub fn add(
        &mut self,
        device: String,
        schema: MeasurementGroup,
    ) -> &mut TsFileSchemaBuilder {
        self.measurement_groups_map.insert(device, schema);
        self
    }

    pub fn build(&mut self) -> Schema {
        // Copy the content
        let mut measurement_groups: HashMap<String, MeasurementGroup> = HashMap::new();
        measurement_groups.clear();
        for (s, mg) in self.measurement_groups_map.iter_mut() {
            measurement_groups.insert(s.clone(), mg.clone());
        }
        Schema { measurement_groups }
    }
}

impl Default for TsFileSchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DeviceBuilder {
    measurement_groups_map: HashMap<String, MeasurementSchema>,
}

impl DeviceBuilder {
    pub fn new() -> DeviceBuilder {
        DeviceBuilder {
            measurement_groups_map: HashMap::new(),
        }
    }

    pub fn add(
        &mut self,
        measurement: String,
        data_type: TSDataType,
        encoding: TSEncoding,
        compression: CompressionType,
    ) -> &mut DeviceBuilder {
        self.measurement_groups_map.insert(
            measurement,
            MeasurementSchema {
                data_type,
                compression,
                encoding,
            },
        );
        self
    }

    pub fn build(&mut self) -> MeasurementGroup {
        assert!(!self.measurement_groups_map.is_empty());
        // Copy the content
        let mut measurement_schemas: HashMap<String, MeasurementSchema> = HashMap::new();
        measurement_schemas.clear();
        for (s, ms) in self.measurement_groups_map.iter_mut() {
            measurement_schemas.insert(s.clone(), ms.clone());
        }
        MeasurementGroup {
            measurement_schemas,
        }
    }
}

impl Default for DeviceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use crate::schema::{DeviceBuilder, TsFileSchemaBuilder};
    use crate::{CompressionType, TSDataType, TSEncoding};

    #[test]
    fn use_fluent_builder() {
        let schema = TsFileSchemaBuilder::new()
            .add(
                "d1",
                DeviceBuilder::new()
                    .add(
                        "s1",
                        TSDataType::INT32,
                        TSEncoding::PLAIN,
                        CompressionType::UNCOMPRESSED,
                    )
                    .add(
                        "s2",
                        TSDataType::INT32,
                        TSEncoding::PLAIN,
                        CompressionType::UNCOMPRESSED,
                    )
                    .build(),
            )
            .build();

        assert_eq!(schema.measurement_groups.len(), 1);
        assert_eq!(
            schema
                .measurement_groups
                .get("d1")
                .unwrap()
                .measurement_schemas
                .len(),
            2
        );
    }
}
