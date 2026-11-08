using System;
using System.Security.Cryptography;
using System.Text;

namespace Lotse.Auth;

/// <summary>The primitives of SCRAM-SHA-512 on raw bytes.</summary>
internal static class Scram
{
    internal static readonly byte[] ClientKeyLabel = Encoding.ASCII.GetBytes("Client Key");
    internal static readonly byte[] ServerKeyLabel = Encoding.ASCII.GetBytes("Server Key");

    internal static byte[] Hmac(byte[] key, byte[] data) => HMACSHA512.HashData(key, data);

    internal static byte[] Sha512(byte[] data) => SHA512.HashData(data);

    /// <summary>PBKDF2-HMAC-SHA512 with a 64-byte output.</summary>
    internal static byte[] SaltedPassword(byte[] password, byte[] salt, int iterations) =>
        Rfc2898DeriveBytes.Pbkdf2(password, salt, iterations, HashAlgorithmName.SHA512, 64);

    internal static byte[] Xor(byte[] a, byte[] b)
    {
        byte[] output = new byte[a.Length];
        for (int i = 0; i < a.Length; i++)
        {
            output[i] = (byte)(a[i] ^ b[i]);
        }
        return output;
    }

    internal static bool ConstantTimeEquals(byte[] a, byte[] b) => CryptographicOperations.FixedTimeEquals(a, b);
}
