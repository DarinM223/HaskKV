module HaskKV.Serialize (Serializable) where

class Serializable s where
    serialize   :: s -> String
    deserialize :: String -> s
