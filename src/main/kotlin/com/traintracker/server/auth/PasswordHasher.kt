package com.traintracker.server.auth

import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Base64
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

/**
 * Password hashing for admin users: PBKDF2-HMAC-SHA256, stored as
 * pbkdf2-sha256$iterations$salt$key (salt and key are unpadded base64).
 * JDK only, so there is no 72-byte password limit.
 *
 * verify() reports needsRehash when the stored iteration count is lower than
 * the current ITERATIONS, so raising ITERATIONS later upgrades each user's
 * hash automatically at their next login.
 */
object PasswordHasher {
    private const val PREFIX = "pbkdf2-sha256"
    private const val ITERATIONS = 600_000 // OWASP guidance for PBKDF2-HMAC-SHA256
    private const val KEY_BITS = 256
    private const val SALT_BYTES = 16
    private const val MAX_PASSWORD_CHARS = 128 // enforced when setting a password
    private const val MAX_VERIFY_CHARS = 1024 // cap on login attempts (CPU abuse guard)

    private val rng = SecureRandom()
    private val enc = Base64.getEncoder().withoutPadding()
    private val dec = Base64.getDecoder()

    data class Result(val ok: Boolean, val needsRehash: Boolean)

    private fun derive(password: String, salt: ByteArray, iterations: Int): ByteArray {
        val spec = PBEKeySpec(password.toCharArray(), salt, iterations, KEY_BITS)
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).encoded
        } finally {
            spec.clearPassword()
        }
    }

    fun hash(password: String): String {
        require(password.length <= MAX_PASSWORD_CHARS) { "Password too long" }
        val salt = ByteArray(SALT_BYTES).also { rng.nextBytes(it) }
        val key = derive(password, salt, ITERATIONS)
        return listOf(PREFIX, ITERATIONS.toString(), enc.encodeToString(salt), enc.encodeToString(key))
            .joinToString("\$")
    }

    fun verify(password: String, stored: String): Result {
        if (password.length > MAX_VERIFY_CHARS) return Result(false, false)
        if (!stored.startsWith(PREFIX + "\$")) return Result(false, false)

        return try {
            val parts = stored.split('$')
            if (parts.size != 4) return Result(false, false)
            val iterations = parts[1].toInt()
            if (iterations < 1) return Result(false, false)
            val expected = dec.decode(parts[3])
            val actual = derive(password, dec.decode(parts[2]), iterations)
            val ok = MessageDigest.isEqual(expected, actual) // constant-time compare
            Result(ok, ok && iterations < ITERATIONS)
        } catch (e: IllegalArgumentException) {
            Result(false, false) // malformed stored hash
        }
    }
}
