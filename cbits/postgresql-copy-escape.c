#include <stddef.h>

/*
 * You might be wondering: why are these functions written in C instead of
 * Haskell?  The answer is two-fold:
 *
 *  * I could not write a Haskell version that was anywhere near as fast as the
 *    C version, short of error-prone buffer traversal with pointers [1].
 *
 *  * The C code is clearer, thanks to C's built-in pointer increment notation,
 *    and to its character literals that are implicitly coerced to
 *    unsigned char.
 *
 *  [1]: http://codereview.stackexchange.com/questions/9998/optimizing-bytestring-escaping
 */

/*
 * Escape a datum for COPY FROM.  The buffer pointed to by @out should be
 * at least 2*in_size bytes long.
 *
 * Return a pointer to the end of the bytes emitted.
 */
unsigned char *c_postgresql_copy_escape_text(
    const unsigned char *in, size_t in_size, unsigned char *out)
{
    while (in_size-- > 0) {
        unsigned char c = *in++;

        /*
         * http://www.postgresql.org/docs/current/static/sql-copy.html#AEN64058
         *
         * "... the following characters must be preceded by a backslash if
         * they appear as part of a column value: backslash itself, newline,
         * carriage return, and the current delimiter character."
         */
        switch (c) {
            case '\t':
                *out++ = '\\';
                *out++ = 't';
                break;
            case '\n':
                *out++ = '\\';
                *out++ = 'n';
                break;
            case '\r':
                *out++ = '\\';
                *out++ = 'r';
                break;
            case '\\':
                *out++ = '\\';
                *out++ = '\\';
                break;

            default:
                *out++ = c;
        }
    }

    return out;
}

/*
 * Like c_postgresql_copy_escape_text, but escape the datum so it will be
 * suitable for PostgreSQL's BYTEA input function.  Note that this does not use
 * the hex format introduced by PostgreSQL 9.0, as it is readable only by
 * PostgreSQL 9.0 and up.
 *
 * This performs two escape operations:
 *
 *  * Convert raw binary data to the format accepted by PostgreSQL's BYTEA
 *    input function.
 *
 *  * Escape the result for use in COPY FROM data.
 *
 * The buffer pointed to by @out should be at least 5*in_size bytes long.
 */
unsigned char *c_postgresql_copy_escape_bytea(
    const unsigned char *in, size_t in_size, unsigned char *out)
{
    while (in_size-- > 0) {
        unsigned char c = *in++;

        if (c == '\\') {
            /* Escape backslash twice, once for BYTEA, and again for COPY FROM. */
            *out++ = '\\';
            *out++ = '\\';
            *out++ = '\\';
            *out++ = '\\';
        } else if (c >= 32 && c <= 126) {
            /*
             * Printable characters (except backslash) are subject to neither
             * BYTEA escaping nor COPY FROM escaping.
             */
            *out++ = c;
        } else {
            /*
             * Escape using octal format.  This consists of two backslashes
             * (single backslash, escaped for COPY FROM) followed by three
             * digits [0-7].
             *
             * We can't use letter escapes \t, \n, \r because:
             *
             *  * The BYTEA input function doesn't understand letter escapes.
             *
             *  * We could use only one backslash so BYTEA sees the literal
             *    octet values of 9, 10, and 13.  However, we're escaping other
             *    non-printable characters for BYTEA; why give 9, 10, and 13
             *    special treatment?
             */
            *out++ = '\\';
            *out++ = '\\';
            *out++ = '0' + ((c >> 6) & 0x7);
            *out++ = '0' + ((c >> 3) & 0x7);
            *out++ = '0' + (c & 0x7);
        }
    }

    return out;
}
