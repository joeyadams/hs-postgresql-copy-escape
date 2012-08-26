{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module Database.PostgreSQL.Copy.Escape (
    EscapeCopyValue(..),
    EscapeCopyRow(..),
    Escape,
    escape,
) where

import Data.ByteString          (ByteString)
import Data.ByteString.Internal (createAndTrim)
import Data.ByteString.Unsafe   (unsafeUseAsCStringLen)
import Data.List                (foldl')
import Data.Monoid
import Foreign
import Foreign.C

import qualified Data.ByteString as B

#if !MIN_VERSION_base(4,5,0)
infixr 6 <>

-- | An infix synonym for 'mappend' (backported to base < 4.5).
(<>) :: Monoid m => m -> m -> m
(<>) = mappend
{-# INLINE (<>) #-}
#endif


-- | An action that takes an input buffer, and for each byte, writes
--   one or more bytes to an output buffer.
type Escaper
    = Ptr CUChar        -- ^ const unsigned char *in
   -> CSize             -- ^ size_t in_size
   -> Ptr CUChar        -- ^ unsigned char *out
   -> IO (Ptr CUChar)   -- ^ Returns pointer to end of written data

foreign import ccall unsafe
    c_postgresql_copy_escape_text :: Escaper

foreign import ccall unsafe
    c_postgresql_copy_escape_bytea :: Escaper

-- | Action that writes bytes into a buffer, returning the new write position.
-- This does not check the buffer size.
newtype Emit = Emit (Ptr CUChar -> IO (Ptr CUChar))

instance Monoid Emit where
    mempty =
        Emit return
    mappend (Emit a) (Emit b) =
        Emit (\ptr0 -> a ptr0 >>= b)

runEmit :: Int -> Emit -> IO ByteString
runEmit bufsize (Emit f) =
    createAndTrim bufsize $ \ptr0 -> do
        ptr1 <- f (castPtr ptr0)
        let len = ptr1 `minusPtr` ptr0
        if len < 0 then
            error "Database.PostgreSQL.Copy.Escape.runEmit: len < 0"
        else if len > bufsize then
            error "Database.PostgreSQL.Copy.Escape.runEmit: buffer overflow"
        else
            return len

emitByte :: CUChar -> Emit
emitByte c = Emit $ \ptr -> do
    pokeElemOff ptr 0 c
    return $! (ptr `plusPtr` 1)

emitEscape :: Escaper -> ByteString -> Emit
emitEscape escaper bs = Emit $ \outptr ->
    unsafeUseAsCStringLen bs $ \(inptr, inlen) ->
        escaper (castPtr inptr) (fromIntegral inlen) outptr

class Escape a where
    escapeEmit :: a -> Emit

    -- | Find an upper bound on the number of bytes
    -- 'escapeEmit' will emit for this value.
    escapeUpperBound :: a -> Int

escape :: Escape a => a -> IO ByteString
escape a = runEmit (escapeUpperBound a) (escapeEmit a)

data EscapeCopyValue
    = EscapeCopyNull
    | EscapeCopyText    !ByteString
        -- ^ A PostgreSQL datum in its text representation.
    | EscapeCopyBytea   !ByteString
        -- ^ Raw binary data destined for storage in a @BYTEA@ column.
    deriving Show

instance Escape EscapeCopyValue where
    escapeEmit v = case v of
        EscapeCopyNull      -> emitByte 92  -- '\\'
                            <> emitByte 78  -- 'N'
        EscapeCopyText  bs  -> emitEscape c_postgresql_copy_escape_text  bs
        EscapeCopyBytea bs  -> emitEscape c_postgresql_copy_escape_bytea bs

    escapeUpperBound v = case v of
        EscapeCopyNull      -> 2
        EscapeCopyText  bs  -> B.length bs * 2
        EscapeCopyBytea bs  -> B.length bs * 5

newtype EscapeCopyRow = EscapeCopyRow [EscapeCopyValue]
    deriving Show

-- | Delimits values with tabs, and adds a newline at the end.
instance Escape EscapeCopyRow where
    escapeEmit (EscapeCopyRow list) =
        case list of
            []     -> newline
            (x:xs) -> escapeEmit x <> go xs
      where
        go []     = newline
        go (x:xs) = tab <> escapeEmit x <> go xs

        tab     = emitByte 9
        newline = emitByte 10

    escapeUpperBound (EscapeCopyRow list) =
        case list of
            []  -> 1
            xs  -> foldl' f 0 xs
      where
        f a x = a + escapeUpperBound x + 1
