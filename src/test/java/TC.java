import com.hft.crypto.EvmCrypto; import java.math.BigInteger; import java.util.*;
/** Тестовый крипто-слой: настоящий keccak, фиктивная подпись, транзакция кодируется в читаемую строку. */
public class TC implements EvmCrypto {
  public final List<byte[]> signed = new ArrayList<>();
  public byte[] keccak256(byte[] d){ return K.keccak256(d); }
  public Signature sign(byte[] h){ signed.add(h); byte[] r=new byte[32], s=new byte[32]; Arrays.fill(r,(byte)0x11); Arrays.fill(s,(byte)0x22); return new Signature(r,s,27); }
  public String address(){ return "0x00000000000000000000000000000000000000aa"; }
  public String signTransaction(long chainId, BigInteger nonce, BigInteger gp, BigInteger gl, String to, BigInteger v, String data){ return "0xRAW|"+chainId+"|"+nonce+"|"+to+"|"+data; }
}
