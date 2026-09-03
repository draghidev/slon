namespace Slon.Runtime.CompilerServices;

// A byref to this wrapper is proven stack-only, allowing non-inlined callees to store
// reference-bearing values without checked write barriers.
ref struct StackValue<T>(T value) where T : allows ref struct
{
    public T Value = value;
}
