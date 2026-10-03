package com.hft.crypto;

import java.math.BigInteger;

/** Минимум EIP-712 для Hyperliquid: домен «Exchange» и структура Agent(string source, bytes32 connectionId). */
public final class Eip712 {
    private Eip712() {}

    private static final byte[] DOMAIN_TYPEHASH = Keccak.hash256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    private static final byte[] AGENT_TYPEHASH = Keccak.hash256("Agent(string source,bytes32 connectionId)");

    public static byte[] domainSeparator(String name, String version, long chainId, byte[] verifyingContract20) {
        return Keccak.hash256(Hex.concat(DOMAIN_TYPEHASH, Keccak.hash256(name), Keccak.hash256(version),
                Hex.fixed(BigInteger.valueOf(chainId), 32), Hex.fixed(new BigInteger(1, verifyingContract20), 32)));
    }

    public static byte[] agentStructHash(String source, byte[] connectionId32) {
        return Keccak.hash256(Hex.concat(AGENT_TYPEHASH, Keccak.hash256(source), connectionId32));
    }

    public static byte[] digest(byte[] domainSeparator, byte[] structHash) {
        return Keccak.hash256(Hex.concat(new byte[]{0x19, 0x01}, domainSeparator, structHash));
    }
}
