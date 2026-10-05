package com.hft.crypto;

import org.web3j.crypto.Credentials;
import org.web3j.crypto.Hash;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.Sign;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.utils.Numeric;

import java.math.BigInteger;

/** Реализация на web3j (зависимость org.web3j:crypto в pom.xml). Приватный ключ — только из application.yml (keys). */
public final class Web3jCrypto implements EvmCrypto {

    /** Ключевая пара web3j из приватного ключа. */
    private final Credentials credentials;

    /** @param privateKeyHex приватный ключ кошелька в hex (с 0x или без) */
    public Web3jCrypto(String privateKeyHex) {
        this.credentials = Credentials.create(privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex);
    }

    /** Keccak-256 (web3j Hash.sha3). */
    @Override
    public byte[] keccak256(byte[] data) { return Hash.sha3(data); }

    /** Подпись хеша без префикса Ethereum Signed Message (для EIP-712), v = 27/28. */
    @Override
    public Signature sign(byte[] hash32) {
        Sign.SignatureData sd = Sign.signMessage(hash32, credentials.getEcKeyPair(), false);
        byte[] v = sd.getV();
        return new Signature(sd.getR(), sd.getS(), v[v.length - 1] & 0xff);
    }

    /** Адрес кошелька 0x… нижним регистром. */
    @Override
    public String address() { return credentials.getAddress().toLowerCase(); }

    @Override
    public String signTransaction(long chainId, BigInteger nonce, BigInteger gasPrice, BigInteger gasLimit,
                                  String to, BigInteger value, String dataHex) {
        RawTransaction tx = RawTransaction.createTransaction(nonce, gasPrice, gasLimit, to, value, dataHex);
        return Numeric.toHexString(TransactionEncoder.signMessage(tx, chainId, credentials));
    }
}
