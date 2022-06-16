pub struct Murmur128 {}

impl Murmur128 {
    /**
     * get hashcode of value by seed
     *
     * @param value value
     * @param seed seed
     * @return hashcode of value
     */
    pub fn hash(value: &String, seed: i32) -> i32 {
        return Self::inner_hash(
            value.as_bytes(),
            0,
            value.as_bytes().len() as i32,
            seed as i64,
        ) as i32;
    }

    /** Methods to perform murmur 128 hash. */
    fn get_block(key: &[u8], offset: usize, index: i32) -> i64 {
        let i8: usize = (index << 3) as usize;
        let block_offset: usize = (offset + i8) as usize;
        return (key[block_offset] as i64 & 0xff)
            + ((key[block_offset + 1] as i64 & 0xff) << 8)
            + ((key[block_offset + 2] as i64 & 0xff) << 16)
            + ((key[block_offset + 3] as i64 & 0xff) << 24)
            + ((key[block_offset + 4] as i64 & 0xff) << 32)
            + ((key[block_offset + 5] as i64 & 0xff) << 40)
            + ((key[block_offset + 6] as i64 & 0xff) << 48)
            + ((key[block_offset + 7] as i64 & 0xff) << 56);
    }

    fn rotl64(v: i64, n: i64) -> i64 {
        (v << n) | ((v as u64) >> (64 - n)) as i64
    }

    #[allow(overflowing_literals)]
    fn fmix(mut k: i64) -> i64 {
        k ^= ((k as u64) >> 33) as i64;
        k = ((k as i128) * 0xff51afd7ed558ccd) as i64;
        k ^= ((k as u64) >> 33) as i64;
        k = ((k as i128) * 0xc4ceb9fe1a85ec53) as i64;
        k ^= ((k as u64) >> 33) as i64;
        return k;
    }

    #[allow(overflowing_literals, arithmetic_overflow)]
    fn inner_hash(key: &[u8], mut offset: usize, length: i32, seed: i64) -> i64 {
        let nblocks = length >> 4; // Process as 128-bit blocks.
        let mut h1 = seed;
        let mut h2 = seed;
        let c1 = 0x87c37b91114253d5;
        let c2 = 0x4cf5ad432745937f;
        // ----------
        // body
        for i in 0..nblocks {
            let mut k1 = Self::get_block(key, offset, i * 2);
            let mut k2 = Self::get_block(key, offset, i * 2 + 1);
            k1 *= c1;
            k1 = Self::rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = Self::rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= c2;
            k2 = Self::rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = Self::rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
        // ----------
        // tail
        // Advance offset to the unprocessed tail of the data.
        offset += nblocks as usize * 16;
        let mut k1 = 0;
        let mut k2 = 0;
        let mut identifier = length & 15;
        // Dirty trick to simulate fallthrough in javas case
        while identifier > 0 {
            match identifier {
                15 => {
                    k2 ^= (key[offset + 14] as i64) << 48;
                }
                14 => {
                    k2 ^= (key[offset + 13] as i64) << 40;
                }
                13 => {
                    k2 ^= (key[offset + 12] as i64) << 32;
                }
                12 => {
                    k2 ^= (key[offset + 11] as i64) << 24;
                }
                11 => {
                    k2 ^= (key[offset + 10] as i64) << 16;
                }
                10 => {
                    k2 ^= (key[offset + 9] as i64) << 8;
                }
                9 => {
                    k2 ^= key[offset + 8] as i64;
                    k2 = ((k2 as i128) * (c2 as i128)) as i64;
                    k2 = Self::rotl64(k2, 33);
                    k2 = ((k2 as i128) * (c1 as i128)) as i64;
                    h2 ^= k2;
                }
                8 => {
                    k1 ^= (key[offset + 7] as i64) << 56;
                }
                7 => {
                    k1 ^= (key[offset + 6] as i64) << 48;
                }
                6 => {
                    k1 ^= (key[offset + 5] as i64) << 40;
                }
                5 => {
                    k1 ^= (key[offset + 4] as i64) << 32;
                }
                4 => {
                    k1 ^= (key[offset + 3] as i64) << 24;
                }
                3 => {
                    k1 ^= (key[offset + 2] as i64) << 16;
                }
                2 => {
                    k1 ^= (key[offset + 1] as i64) << 8;
                }
                1 => {
                    k1 ^= key[offset] as i64;
                    k1 = ((k1 as i128) * (c1 as i128)) as i64;
                    k1 = Self::rotl64(k1, 31);
                    k1 = ((k1 as i128) * (c2 as i128)) as i64;
                    h1 ^= k1;
                }
                _ => {}
            }
            identifier -= 1
        }
        // ----------
        // finalization
        h1 ^= length as i64;
        h2 ^= length as i64;
        h1 += h2;
        h2 += h1;
        h1 = Self::fmix(h1);
        h2 = Self::fmix(h2);
        h1 = ((h1 as i128) + (h2 as i128)) as i64;
        h2 = ((h2 as i128) + (h1 as i128)) as i64;
        return h1.overflowing_add(h2).0;
    }
}
