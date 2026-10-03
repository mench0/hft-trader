package com.hft.crypto;

import org.web3j.crypto.Credentials;
import org.web3j.crypto.Hash;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.Sign;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.utils.Numeric;

import java.math.BigInteger;

/** Реализация на web3j (зависимость org.web3j:crypto в pom.xml). Приватный ключ — только из переменной окружения. */
public final class Web3jCrypto implements EvmCrypto {

    private final Credentials credentials;

    public Web3jCrypto(String privateKeyHex) {
        this.credentials = Credentials.create(privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex);
    }

    @Override
    public byte[] keccak256(byte[] data) { return Hash.sha3(data); }

    @Override
    public Signature sign(byte[] hash32) {
        Sign.SignatureData sd = Sign.signMessage(hash32, credentials.getEcKeyPair(), false);
        byte[] v = sd.getV();
        return new Signature(sd.getR(), sd.getS(), v[v.length - 1] & 0xff);
    }

    @Override
    public String address() { return credentials.getAddress().toLowerCase(); }

    @Override
    public String signTransaction(long chainId, BigInteger nonce, BigInteger gasPrice, BigInteger gasLimit,
                                  String to, BigInteger value, String dataHex) {
        RawTransaction tx = RawTransaction.createTransaction(nonce, gasPrice, gasLimit, to, value, dataHex);
        return Numeric.toHexString(TransactionEncoder.signMessage(tx, chainId, credentials));
    }
}
