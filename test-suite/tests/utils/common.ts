export function isDefined<T>(inp: T | undefined | null): inp is T {
    return inp != null;
}

export function expectToBeOrdered(inp: string[]) {
    expect(inp).toEqual(inp.slice().sort());
}
