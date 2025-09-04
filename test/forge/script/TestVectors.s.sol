// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "forge-std/Script.sol";

contract TestVectors is Script {
    
    function run() public view {
        testKeccak256();
        testCreate2Predict();
        testCreate2Mine();
        testFormatFunctions();
    }

    function testKeccak256() private pure {
        console.log("KECCAK256 TESTS\n");
        
        bytes32 result1 = keccak256("");
        require(
            result1 == 0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470,
            "Test 1 failed"
        );
        console.log("  Empty string: 0x%s", toHexString(result1));
        
        bytes32 result2 = keccak256("hello world");
        require(
            result2 == 0x47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad,
            "Test 2 failed"
        );
        console.log("  hello world: 0x%s", toHexString(result2));
        
        bytes32 result3 = keccak256(hex"deadbeef");
        require(
            result3 == 0xd4fd4e189132273036449fc9e11198c739161b4c0116a9a2dccdfa1c492006f1,
            "Test 3 failed"
        );
        console.log("  0xdeadbeef: 0x%s", toHexString(result3));
        
        bytes32 result4 = keccak256(hex"00");
        require(
            result4 == 0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a,
            "Test 4 failed"
        );
        console.log("  0x00: 0x%s", toHexString(result4));
        
        bytes32 result5 = keccak256(
            hex"0000000000000000000000000000000000000000000000000000000000000000"
        );
        require(
            result5 == 0x290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563,
            "Test 5 failed"
        );
        console.log("  32 zero bytes: 0x%s", toHexString(result5));
        
        bytes32 result6 = keccak256("The quick brown fox jumps over the lazy dog");
        require(
            result6 == 0x4d741b6f1eb29cb2a9b9911c82f56fa8d73b04959d3d9d222895df6c0b28aa15,
            "Test 6 failed"
        );
        console.log("  Pangram: 0x%s", toHexString(result6));
        
        bytes32 result7 = keccak256(hex"f09fa686");
        require(
            result7 == 0xaed18ec64cd5075f8fc3d8caf6c467c754b44bd892dec0206f5e80ab190aa744,
            "Test 7 failed"
        );
        console.log("  Duck emoji (0xf09fa686): 0x%s", toHexString(result7));
        
        bytes32 result8 = keccak256(
            hex"000000000000000000000000000000000000000000000000000000000000007b"
        );
        require(
            result8 == 0x5569044719a1ec3b04d0afa9e7a5310c7c0473331d13dc9fafe143b2c4e8148a,
            "Test 8 failed"
        );
        console.log("  uint256(123): 0x%s", toHexString(result8));
        
        bytes32 result9 = keccak256(hex"0123");
        require(
            result9 == 0x667d3611273365cfb6e64399d5af0bf332ec3e5d6986f76bc7d10839b680eb58,
            "Test 9 failed"
        );
        console.log("  0x0123: 0x%s", toHexString(result9));
    }

    function testCreate2Predict() private pure {
        console.log("\nCREATE2 PREDICT TESTS\n");
        
        address result1 = computeCreate2Address(
            0x0000000000000000000000000000000000000001,
            bytes32(0),
            0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a
        );
        require(result1 == 0xc660A83B541fde5aEd091688b9E015d7e6B8AC61, "Test 1 failed");
        console.log("  Minimal values: %s", result1);
        
        address result2 = computeCreate2Address(
            0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266,
            bytes32(uint256(0x3039)),
            0xef67bed7d52b5d7b41c79c16d9b9ea93946005914ea3213b7fc08c62555eaaf4
        );
        require(result2 == 0x644068E837AAf6B821a2C22F4e97e08cCBaCbB11, "Test 2 failed");
        console.log("  Foundry test: %s", result2);
        
        address result3 = computeCreate2Address(
            0x4e59b44847b379578588920cA78FbF26c0B4956C,
            bytes32(uint256(0xdeadbeef)),
            0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a
        );
        require(result3 == 0xb76e437E42C2b0673c1946Bb97cb337f6B6a3339, "Test 3 failed");
        console.log("  Deterministic deployer: %s", result3);
        
        address result4 = computeCreate2Address(
            0xFFfFfFffFFfffFFfFFfFFFFFffFFFffffFfFFFfF,
            bytes32(type(uint256).max),
            bytes32(type(uint256).max)
        );
        require(result4 == 0x29F2ea9EdCE8696dcFbE1c9dCC90DE77A7Bd252D, "Test 4 failed");
        console.log("  Max values: %s", result4);
        
        address result5 = computeCreate2Address(
            0x0000000000000000000000000000000000000001,
            bytes32(uint256(1)),
            0xbd1ab8973fc701940fe1252ea00647970bf77e9e293777b24d10d4247adb44f1
        );
        require(result5 == 0xAFCda66BEdACd76e434Da13DbbE6c46BD92cC838, "Test 5 failed");
        console.log("  Minimal proxy: %s", result5);
        
        address result6 = computeCreate2Address(
            0x1234567890123456789012345678901234567890,
            bytes32(0x00000000000000000000000000000000000000000000000000000000cafebabe),
            0x1c3374235d773b2189aed115aa13143020fcdbbe86e38f358cf3e4771b2f0244
        );
        require(result6 == 0x057795d6B3891F5805d67BC94ACf36789cC3871c, "Test 6 failed");
        console.log("  Salt pattern: %s", result6);
        
        address result7a = computeCreate2Address(
            0x0000000000000000000000000000000000000001,
            bytes32(uint256(123)),
            0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a
        );
        address result7b = computeCreate2Address(
            0x0000000000000000000000000000000000000001,
            bytes32(uint256(0x7b)),
            0xbc36789e7a1e281436464229828f817d6612f7b477d66591ff96a9e064bcc98a
        );
        require(result7a == result7b, "Test 7 failed");
        console.log("  Salt 123 == 0x7b: %s", result7a);
    }

    function testCreate2Mine() private pure {
        console.log("\nCREATE2 MINE TESTS\n");
        
        address deployer = 0x4e59b44847b379578588920cA78FbF26c0B4956C;
        bytes32 initCodeHash = 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef;
        
        for (uint256 i = 0; i < 5; i++) {
            address mined = computeCreate2Address(
                deployer,
                bytes32(i),
                initCodeHash
            );
            console.log("  Salt %s: %s", i, mined);
        }
        
        address addr1000 = computeCreate2Address(
            deployer,
            bytes32(uint256(1000)),
            initCodeHash
        );
        console.log("  Salt 1000: %s", addr1000);
        
        address addrMax64 = computeCreate2Address(
            deployer,
            bytes32(uint256(18446744073709551615)),
            initCodeHash
        );
        console.log("  Salt max uint64: %s", addrMax64);
    }

    function testFormatFunctions() private pure {
        console.log("\nFORMAT FUNCTION TESTS\n");
        
        bytes32 salt1 = formatSalt(0, 0);
        require(salt1 == bytes32(0), "Format salt test 1 failed");
        console.log("  format_salt(0, 0): 0x%s", toHexString(salt1));
        
        bytes32 salt2 = formatSalt(0, 123);
        require(salt2 == bytes32(uint256(123)), "Format salt test 2 failed");
        console.log("  format_salt(0, 123): 0x%s", toHexString(salt2));
        
        bytes32 salt3 = formatSalt(1, 0);
        require(salt3 == bytes32(uint256(1) << 128), "Format salt test 3 failed");
        console.log("  format_salt(1, 0): 0x%s", toHexString(salt3));
        
        uint64 maxUint64 = 18446744073709551615;
        bytes32 salt4 = formatSalt(maxUint64, maxUint64);
        console.log("  format_salt(max, max): 0x%s", toHexString(salt4));
        
        bytes32 salt5 = formatSalt(0, 1000);
        require(salt5 == bytes32(uint256(1000)), "Format salt test 5 failed");
        console.log("  format_salt(0, 1000): 0x%s", toHexString(salt5));
        
        console.log("\n");
        
        address addr1 = formatAddress(0, 0, 0);
        require(addr1 == address(0), "Format address test 1 failed");
        console.log("  format_address(0, 0, 0): %s", addr1);
        
        address addr2 = formatAddress(0, 0, 1);
        require(addr2 == address(1), "Format address test 2 failed");
        console.log("  format_address(0, 0, 1): %s", addr2);
        
        address addr3 = formatAddress(1, 0, 0);
        require(addr3 == address(uint160(1) << 96), "Format address test 3 failed");
        console.log("  format_address(1, 0, 0): %s", addr3);
        
        address addr4 = formatAddress(
            maxUint64,
            maxUint64,
            type(uint32).max
        );
        require(addr4 == address(type(uint160).max), "Format address test 4 failed");
        console.log("  format_address(max, max, max): %s", addr4);
        
        address addr5 = formatAddress(0, 0, 0xdeadbeef);
        require(addr5 == address(uint160(0xdeadbeef)), "Format address test 5 failed");
        console.log("  format_address(0, 0, 0xdeadbeef): %s", addr5);
    }

    function computeCreate2Address(
        address deployer,
        bytes32 salt,
        bytes32 initCodeHash
    ) private pure returns (address) {
        return address(
            uint160(
                uint256(
                    keccak256(
                        abi.encodePacked(
                            bytes1(0xff),
                            deployer,
                            salt,
                            initCodeHash
                        )
                    )
                )
            )
        );
    }

    function formatSalt(uint64 hi, uint64 lo) private pure returns (bytes32) {
        return bytes32((uint256(hi) << 128) | uint256(lo));
    }

    function formatAddress(
        uint64 hi8,
        uint64 mid8,
        uint32 lo4
    ) private pure returns (address) {
        uint160 result = (uint160(hi8) << 96) | 
                        (uint160(mid8) << 32) | 
                        uint160(lo4);
        return address(result);
    }

    function toHexString(bytes32 data) private pure returns (string memory) {
        bytes memory alphabet = "0123456789abcdef";
        bytes memory str = new bytes(64);
        
        for (uint256 i = 0; i < 32; i++) {
            str[i * 2] = alphabet[uint8(data[i] >> 4)];
            str[1 + i * 2] = alphabet[uint8(data[i] & 0x0f)];
        }
        
        return string(str);
    }
}