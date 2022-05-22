enum IoTDBValue {
    DOUBLE(f64),
    FLOAT(f32),
    INT(i32)
}

struct Chunk<T> {
    raw_value: Option<T>,
}

trait Chunkeable {
    fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()>;
}

impl Chunkeable for Chunk<i32> {
    fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()> {
        return match value {
            IoTDBValue::INT(inner) => {
                self.raw_value = Some(inner);
                Ok(())
            }
            _ => {
                Err(())
            }
        }
    }
}

impl Chunkeable for Chunk<f32> {
    fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()> {
        return match value {
            IoTDBValue::FLOAT(inner) => {
                self.raw_value = Some(inner);
                Ok(())
            }
            _ => {
                Err(())
            }
        }
    }
}

impl Chunkeable for Chunk<f64> {
    fn set_raw(&mut self, value: IoTDBValue) -> Result<(), ()> {
        return match value {
            IoTDBValue::DOUBLE(inner) => {
                self.raw_value = Some(inner);
                Ok(())
            }
            _ => {
                Err(())
            }
        }
    }
}

impl<T> Chunk<T> {
    fn new() -> Chunk<T> {
        return Chunk {
            raw_value: None,
        };
    }
}

struct ChunkGroup {
    chunks: Vec<Box<dyn Chunkeable>>
}

#[cfg(test)]
mod testsabc {
    use std::ops::{DerefMut};

    use crate::test::{Chunk, ChunkGroup, IoTDBValue};

    #[test]
    fn it_works() {
        let chunk: Chunk<i32> = Chunk::new();
        let mut cg = ChunkGroup {
            chunks: vec![Box::new(Chunk::<i32>::new()),
                         Box::new(Chunk::<f32>::new()),
                         Box::new(Chunk::<f64>::new())
            ]
        };

        match cg.chunks.get_mut(0) {
            None => {}
            Some(chunk) => {
                chunk.deref_mut().set_raw(IoTDBValue::INT(13));
            }
        }
        match cg.chunks.get_mut(1) {
            None => {}
            Some(chunk) => {
                chunk.set_raw(IoTDBValue::FLOAT(13.0));
            }
        }
        match cg.chunks.get_mut(2) {
            None => {}
            Some(chunk) => {
                chunk.set_raw(IoTDBValue::DOUBLE(13.0));
            }
        }
    }

}
