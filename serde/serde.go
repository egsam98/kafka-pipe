package serde

type Deserializer interface {
	Deserialize(dst any, src []byte) error
}
