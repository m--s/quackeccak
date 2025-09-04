# quackeccak

> ⚠️ **EXPERIMENTAL**: This extension is in early development. APIs may change without notice. Not for production use.

A DuckDB extension for Ethereum blockchain computations - bringing essential EVM operations like Keccak-256 hashing and CREATE2 address mining directly to your SQL queries. Analyze smart contracts and optimize gas costs without leaving your database.

## Features

### `keccak256(input)`

Computes the Keccak-256 hash, the fundamental cryptographic function in Ethereum used for everything from generating addresses to verifying data integrity.

```sql
SELECT keccak256('hello world');
-- Returns: 0x47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad
```

- Accepts text strings or hex bytes (prefixed with `0x`)
- Returns lowercase hex string with `0x` prefix
- Uses the [XKCP Keccak implementation](https://github.com/XKCP/XKCP)

### `create2_predict(deployer, salt, init_hash)`

Predicts the deterministic address of a smart contract deployed via CREATE2 opcode.

```sql
SELECT create2_predict(
    '0x4e59b44847b379578588920ca78fbf26c0b4956c',  -- deployer/factory address
    '0xdeadbeef',                                   -- salt value
    '0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a'  -- keccak256(init_code)
);
-- Returns: 0xb76e437e42c2b0673c1946bb97cb337f6b6a3339
```

### `create2_mine(...)`

Table function that mines CREATE2 salts to find contract addresses matching specific patterns. Essential for gas optimization and protocol requirements in EVM-based blockchains.

**Parameters:**

- `deployer` (VARCHAR): Deploying contract address
- `init_hash` (VARCHAR): Keccak256 hash of initialization bytecode
- `salt_start` (UBIGINT): Starting salt value
- `salt_count` (UBIGINT): Number of salts to test
- `mask_hi8` (UBIGINT): Bitmask for address bytes 0-7
- `value_hi8` (UBIGINT): Desired values for masked bits
- `mask_mid8` (UBIGINT): Bitmask for address bytes 8-15
- `value_mid8` (UBIGINT): Desired values for masked bits
- `mask_lo4` (UINTEGER): Bitmask for address bytes 16-19
- `value_lo4` (UINTEGER): Desired values for masked bits
- `max_results` (UBIGINT): Maximum results to return

## Use Cases

### Gas Optimization for Smart Contracts

Each zero byte in an Ethereum address saves 4 gas compared to non-zero bytes. Mining addresses with leading zeros can reduce transaction costs significantly:

```sql
-- Find addresses starting with 0x0000 (saves 8 gas per transaction)
SELECT * FROM create2_mine(
    '0x4e59b44847b379578588920ca78fbf26c0b4956c',
    '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',
    '0',          -- start from salt 0
    '10000000',   -- test 10 million salts
    '0xffff000000000000', '0x0000000000000000',  -- mask first 2 bytes, require zeros
    '0', '0',     -- don't check middle bytes
    '0', '0',     -- don't check last bytes
    '10'          -- return max 10 results
);
```

### Storage Slot Optimization

EVM storage slots are 256 bits, but addresses are only 160 bits. By mining addresses with leading zeros, you can pack multiple addresses into a single storage slot:

```sql
-- Mine for 4 leading zero bytes (allows packing 2 addresses per slot)
-- Warning: This requires extensive computation
SELECT * FROM create2_mine(
    '0x4e59b44847b379578588920ca78fbf26c0b4956c',
    '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',
    '0', '1000000000',  -- test 1 billion salts (be patient!)
    '0xffffffff00000000', '0x0000000000000000',  -- mask = check 4 bytes, value = all zeros
    '0', '0', '0', '0',
    '1'  -- return first match only
);
```

### Uniswap V4 Hook Addresses

Uniswap V4 encodes hook permissions directly in the contract address. The protocol checks specific bits to determine which hooks are enabled:

```sql
-- Mine address with BEFORE_SWAP (bit 7) and AFTER_SWAP (bit 6) hooks
SELECT * FROM create2_mine(
    '0x4e59b44847b379578588920ca78fbf26c0b4956c',
    '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
    '0', '1000000',
    '0', '0',              -- don't check high bytes
    '0', '0',              -- don't check middle bytes
    '0x000000c0',          -- mask: check bits 7 and 6 (0x80 + 0x40 = 0xc0)
    '0x000000c0',          -- value: both bits must be set
    '1'
);

-- Common hook combinations:
-- BEFORE_SWAP only: mask=0x00000080, value=0x00000080
-- AFTER_SWAP only: mask=0x00000040, value=0x00000040
-- Both swap hooks: mask=0x000000c0, value=0x000000c0
```

### Vanity Addresses for DeFi Protocols

Create memorable addresses for better user recognition and branding:

```sql
-- Find addresses ending with 'cafe' (last 2 bytes)
SELECT * FROM create2_mine(
    '0x4e59b44847b379578588920ca78fbf26c0b4956c',
    '0x5555555555555555555555555555555555555555555555555555555555555555',
    '0', '10000000',
    '0', '0',              -- don't check first bytes
    '0', '0',              -- don't check middle bytes
    '0x0000ffff',          -- mask: check last 2 bytes (positions 18-19)
    '0x0000cafe',          -- value: must be 'cafe'
    '5'                    -- return up to 5 matches
);
```

## Contributing

This project focuses on local blockchain analysis tools for DuckDB. Bug reports, feature requests, and contributions are welcome!

## License

MIT
