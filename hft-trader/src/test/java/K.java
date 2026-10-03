import java.util.*;
/** Тестовая реализация Keccak-256 (только для проверки, в проекте используется web3j). */
public final class K {
  static final long[] RC = {0x0000000000000001L,0x0000000000008082L,0x800000000000808aL,0x8000000080008000L,0x000000000000808bL,0x0000000080000001L,0x8000000080008081L,0x8000000000008009L,0x000000000000008aL,0x0000000000000088L,0x0000000080008009L,0x000000008000000aL,0x000000008000808bL,0x800000000000008bL,0x8000000000008089L,0x8000000000008003L,0x8000000000008002L,0x8000000000000080L,0x000000000000800aL,0x800000008000000aL,0x8000000080008081L,0x8000000000008080L,0x0000000080000001L,0x8000000080008008L};
  static final int[] ROT = {1,3,6,10,15,21,28,36,45,55,2,14,27,41,56,8,25,43,62,18,39,61,20,44};
  static final int[] PI = {10,7,11,17,18,3,5,16,8,21,24,4,15,23,19,13,12,2,20,14,22,9,6,1};
  static void f(long[] s){ long[] c=new long[5]; for(int r=0;r<24;r++){
    for(int x=0;x<5;x++) c[x]=s[x]^s[x+5]^s[x+10]^s[x+15]^s[x+20];
    for(int x=0;x<5;x++){ long d=c[(x+4)%5]^Long.rotateLeft(c[(x+1)%5],1); for(int y=0;y<25;y+=5) s[y+x]^=d; }
    long t=s[1]; for(int i=0;i<24;i++){ int j=PI[i]; long tmp=s[j]; s[j]=Long.rotateLeft(t,ROT[i]); t=tmp; }
    for(int y=0;y<25;y+=5){ long[] a=new long[5]; for(int x=0;x<5;x++) a[x]=s[y+x]; for(int x=0;x<5;x++) s[y+x]=a[x]^((~a[(x+1)%5])&a[(x+2)%5]); }
    s[0]^=RC[r]; } }
  public static byte[] keccak256(byte[] in){ int rate=136; long[] s=new long[25];
    int len=in.length; byte[] p=Arrays.copyOf(in, ((len/rate)+1)*rate); p[len]^=0x01; p[p.length-1]^=(byte)0x80;
    for(int off=0;off<p.length;off+=rate){ for(int i=0;i<rate/8;i++){ long v=0; for(int b=7;b>=0;b--) v=(v<<8)|(p[off+i*8+b]&0xff); s[i]^=v; } f(s); }
    byte[] out=new byte[32]; for(int i=0;i<32;i++) out[i]=(byte)(s[i/8]>>>(8*(i%8))); return out; }
  public static String hex(byte[] b){ return HexFormat.of().formatHex(b); }
}
