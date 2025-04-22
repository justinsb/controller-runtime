package diskcache

type Object interface {
	GetNamespace() string
	GetName() string
}

// type ObjectList[T Object] interface {
// 	Len() int
// 	Get(i int) *T
// }

// type TypedReader[ObjectT Object] interface {
// 	List(ctx context.Context, list ObjectList, opts ...ListOption) error
// 	Watch(ctx context.Context, list ObjectList, opts ...ListOption) error
// }
