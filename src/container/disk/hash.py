import mmh3


class HashFunction:
    @classmethod
    def get_hash(cls, key) -> int:
        # Convert the key to bytes if it is not already
        if isinstance(key, str):
            key = key.encode("utf-8")
        elif isinstance(key, int):
            key = key.to_bytes((key.bit_length() + 7) // 8, byteorder="little")
        elif isinstance(key, bytes):
            pass
        else:
            raise TypeError("Unsupported key type")

        # Get the 128-bit hash using MurmurHash3
        hash128 = mmh3.hash128(key, seed=0)
        return hash128 & 0xFFFFFFFFFFFFFFFF  # Return only the lower 64 bits


# # Example usage
# print(HashFunction.get_hash("example_key"))  # Outputs the hash value of 'example_key'
