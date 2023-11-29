export function isDefined<T>(inp: T | undefined | null): inp is T {
    return inp != null;
}
