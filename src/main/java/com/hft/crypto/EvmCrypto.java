package com.hft.crypto;

import java.math.BigInteger;

/**
 * Всё, что требует secp256k1/keccak, спрятано за этим интерфейсом. Боевая реализация —
 * {@link Web3jCrypto} (библиотека web3j из Maven, самодельной криптографии нет);
 * в тестах подставляется другая реализация.
 */
public interface EvmCrypto {

    /** Keccak-256 (тот, что в Ethereum, не NIST SHA3-256). */
    byte[] keccak256(byte[] data);

    /** Подпись 32-байтного хеша: r (32 байта), s (32 байта), v (27 или 28). */
    Signature sign(byte[] hash32);

    /** Адрес кошелька в виде 0x…, нижний регистр. */
    String address();

    /** Подписанная legacy-транзакция (EIP-155), 0x… hex для eth_sendRawTransaction. */
    String signTransaction(long chainId, BigInteger nonce, BigInteger gasPrice, BigInteger gasLimit,
                           String to, BigInteger value, String dataHex);

    /** Подпись secp256k1: r и s по 32 байта, v — 27 или 28. */
    record Signature(byte[] r, byte[] s, int v) {}
}
