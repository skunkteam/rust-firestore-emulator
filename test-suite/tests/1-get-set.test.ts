import { DocumentReference, FieldPath, FieldValue, Timestamp } from '@google-cloud/firestore';
import {
    RawDocument,
    RawProtoValue,
    callRawUnaryRpc,
    editions,
    expectRpcCode,
    notImplementedInJava,
    notImplementedInRust,
    readData,
    writeData,
} from './utils';

const ONE_HOUR_MS = 60 * 60 * 1_000;

interface RawCommitRequest {
    database: string;
    writes: Array<{
        update: RawDocument;
        updateTransforms?: Array<{
            fieldPath: string;
            appendMissingElements?: { values: RawProtoValue[] };
        }>;
    }>;
}

function rawTtlValue(): RawProtoValue {
    const ttl = Timestamp.fromMillis(Date.now() + ONE_HOUR_MS);
    return { timestampValue: { seconds: ttl.seconds, nanos: ttl.nanoseconds } };
}

describe.each(editions)('$description', fs => {
    let docRef: DocumentReference;
    beforeEach(async () => {
        docRef = fs.collection.doc();
    });

    test('getting a non-existing document', async () => {
        const snap = await docRef.get();
        expect(snap.exists).toBeFalse();
        expect(snap.data()).toBeUndefined();
    });

    describe('creating', () => {
        describe.each([
            { using: 'set', createFn: (data: Record<string, unknown>) => docRef.set(writeData(data)) },
            { using: 'create', createFn: (data: Record<string, unknown>) => docRef.create(writeData(data)) },
        ] as const)('using $using', ({ createFn }) => {
            test('setting and getting a basic document', async () => {
                const data = { foo: 'bar' };
                await createFn(data);

                expect(await getDoc()).toEqual({ foo: 'bar' });
            });

            test('setting and getting a complex document', async () => {
                const data = {
                    boolean: true,
                    number: 3,
                    string: 'Small',
                    obj: {
                        boolean: false,
                        number: 3.14,
                        string: 'Larger string',
                    },
                    arr: [Math.PI, { string: 'Can I get this string back again?' }, Number.MAX_VALUE, Number.MIN_VALUE],
                };
                await createFn(data);

                expect(await getDoc()).toEqual(data);
            });

            test('using serverTimestamp', async () => {
                await createFn({
                    string: 'foo',
                    firstTime: FieldValue.serverTimestamp(),
                    object: {
                        withSecondTime: FieldValue.serverTimestamp(),
                    },
                });

                const doc = await getDoc();

                expect(doc).toEqual({
                    string: 'foo',
                    firstTime: expect.any(Timestamp),
                    object: {
                        withSecondTime: expect.any(Timestamp),
                    },
                });
                // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
                expect(doc?.firstTime).toEqual(doc?.object.withSecondTime); // should be equal including nanoseconds
            });

            test('no serverTimestamp in an Array', async () => {
                const data = { arr: [FieldValue.serverTimestamp()] };
                // Is checked client side apparently
                expect(() => createFn(data)).toThrow('FieldValue.serverTimestamp() cannot be used inside of an array');
            });
        });

        test('can only create once', async () => {
            const data = writeData();
            await docRef.create(data);
            await expect(docRef.create(data)).rejects.toThrow('6 ALREADY_EXISTS');
        });
    });

    test('getAll', async () => {
        const firstData = { some: 'data' };
        const secondData = { some: 'other data' };
        const secRef = fs.collection.doc();
        await Promise.all([docRef.set(writeData(firstData)), secRef.set(writeData(secondData))]);
        const [first, second] = await fs.firestore.getAll(docRef, secRef);
        expect(readData(first.data())).toEqual(firstData);
        expect(readData(second.data())).toEqual(secondData);
    });

    describe('updating', () => {
        const data = writeData();

        beforeEach(async () => {
            await docRef.set(data);
        });

        let currentDoc: Record<string, unknown>;
        beforeEach(() => (currentDoc = data));
        describe.each([
            {
                name: 'set',
                updateFn: async (update: Record<string, unknown>) => {
                    currentDoc = { ...currentDoc, ...update };
                    await docRef.set(currentDoc);
                },
            },
            {
                name: 'merge',
                updateFn: async (update: Record<string, unknown>) => {
                    await docRef.set(update, { merge: true });
                },
            },
            // TODO: mergeFields
            {
                name: 'update',
                updateFn: async (update: Record<string, unknown>) => {
                    await docRef.update(update);
                },
            },
        ] as const)('$name', ({ name, updateFn }) => {
            test('add field', async () => {
                await updateFn({ newField: 'value' });
                expect(await getDoc()).toEqual({
                    newField: 'value',
                });
                await updateFn({ otherField: 42 });
                expect(await getDoc()).toEqual({
                    newField: 'value',
                    otherField: 42,
                });
            });

            test('overwrite existing field', async () => {
                await updateFn({ newField: 'value' });
                await updateFn({ newField: 'other value' });
                expect(await getDoc()).toEqual({ newField: 'other value' });
            });

            describe('FieldValues', () => {
                // When using `set` without `merge`, the `increment`/`arrayUnion`/`arrayRemove` operation will never 'merge' the field, it will
                // just set it to the given value.
                const setOperation = name === 'set';

                test('delete field', async () => {
                    await updateFn({ newField: 'value' });
                    if (setOperation) {
                        // Is checked async!?
                        await expect(updateFn({ newField: FieldValue.delete() })).rejects.toThrow(
                            'FieldValue.delete() must appear at the top-level and can only be used in update() or set() with {merge:true}',
                        );
                    } else {
                        await updateFn({ newField: FieldValue.delete() });
                        expect(await getDoc()).not.toHaveProperty('newField');
                    }
                });

                test('increment', async () => {
                    // `increment` on a non existing value sets the value
                    await updateFn({ counter: FieldValue.increment(1) });
                    expect(await getDoc()).toEqual({ counter: 1 });

                    // When using `set` without `merge`, the `increment` operation will never 'merge' the field, it will just set it to the
                    // increment value.
                    await updateFn({ counter: FieldValue.increment(15) });
                    expect(await getDoc()).toEqual({ counter: setOperation ? 15 : 16 });

                    await updateFn({ counter: FieldValue.increment(-32.1) });
                    expect(await getDoc()).toEqual({ counter: setOperation ? -32.1 : -16.1 });
                });

                test('arrayUnion', async () => {
                    await updateFn({ arr: FieldValue.arrayUnion({ first: 'value' }, 'second') });
                    expect(await getDoc()).toEqual({ arr: [{ first: 'value' }, 'second'] });

                    // It should deduplicate `first`, but add `third`
                    await updateFn({ arr: FieldValue.arrayUnion({ third: 3 }, { first: 'value' }) });
                    if (setOperation) {
                        expect(await getDoc()).toEqual({ arr: [{ third: 3 }, { first: 'value' }] });
                    } else {
                        expect(await getDoc()).toEqual({ arr: [{ first: 'value' }, 'second', { third: 3 }] });
                    }

                    // Cross-type numeric (0 vs -0) and NaN deduplication
                    await updateFn({ arr: [0, NaN, { nested: -0 }] });
                    await updateFn({ arr: FieldValue.arrayUnion(-0, NaN, { nested: 0 }, 42) });
                    if (setOperation) {
                        expect(await getDoc()).toEqual({ arr: [-0, NaN, { nested: 0 }, 42] });
                    } else {
                        expect(await getDoc()).toEqual({ arr: [0, NaN, { nested: -0 }, 42] });
                    }

                    // Deduplicating inputs on missing and non-array scalar fields
                    await updateFn({ scalarArr: 'not-an-array' });
                    await updateFn({
                        missingArr: FieldValue.arrayUnion(1, 1, 0, -0, NaN, NaN),
                        scalarArr: FieldValue.arrayUnion('x', 'x', 'y'),
                    });
                    const afterMissingUnion = await getDoc();
                    expect(afterMissingUnion).toMatchObject({
                        missingArr: [1, 0, NaN],
                        scalarArr: ['x', 'y'],
                    });
                });

                test('arrayRemove', async () => {
                    await updateFn({ arr: [{ first: 'foo' }, { second: 'bar' }, 'third', 'third'] });

                    await updateFn({ arr: FieldValue.arrayRemove({ doesNot: 'exist' }) });
                    expect(await getDoc()).toEqual({ arr: setOperation ? [] : [{ first: 'foo' }, { second: 'bar' }, 'third', 'third'] });

                    await updateFn({ arr: FieldValue.arrayRemove('third', { first: 'foo' }, 'nope') });
                    expect(await getDoc()).toEqual({ arr: setOperation ? [] : [{ second: 'bar' }] });

                    // Cross-type numeric (-0 removed by 0) and NaN removal
                    await updateFn({ arr: [-0, NaN, { nested: -0 }, 42] });
                    await updateFn({ arr: FieldValue.arrayRemove(0, NaN, { nested: 0 }) });
                    expect(await getDoc()).toEqual({ arr: setOperation ? [] : [42] });
                });
            });
        });
        test('updating and merging multi-byte UTF-8 nested field paths', async () => {
            await docRef.set(writeData({ café: { price: 5, keep: true }, aéb: 'before' }));
            await docRef.update(new FieldPath('café', 'price'), 7);
            await docRef.update(new FieldPath('aéb'), 'after');
            expect(await getDoc()).toEqual({ café: { price: 7, keep: true }, aéb: 'after' });
        });

        test('concurrent opposite-order WriteBatch commits succeed without deadlock', async () => {
            const refA = fs.collection.doc();
            const refB = fs.collection.doc();
            await Promise.all([
                fs.firestore
                    .batch()
                    .set(refA, writeData({ by: 'batch1' }))
                    .set(refB, writeData({ by: 'batch1' }))
                    .commit(),
                fs.firestore
                    .batch()
                    .set(refB, writeData({ by: 'batch2' }))
                    .set(refA, writeData({ by: 'batch2' }))
                    .commit(),
            ]);
            const [snapA, snapB] = await fs.firestore.getAll(refA, refB);
            expect(snapA.exists).toBeTrue();
            expect(snapB.exists).toBeTrue();
        });
    });

    describe('deleting', () => {
        test('existing document', async () => {
            await docRef.set(writeData());
            await docRef.delete();
            expect(await docRef.get()).toHaveProperty('exists', false);
        });

        test('non-existing document', async () => {
            expect(await docRef.get()).toHaveProperty('exists', false);
            await docRef.delete();
            expect(await docRef.get()).toHaveProperty('exists', false);
        });
    });

    describe('edge cases', () => {
        describe('keys', () => {
            describe.each([
                {
                    rule: 'must be valid UTF-8 characters',
                    valid: ['X Æ A-Xii', 'Bond, James', 'painting 🌃', '`or backticks`'],
                    // Remove 1 character from the last emoji, to create invalid UTF8
                    invalid: ['🐶🐶🐶'.substring(0, 5)],
                    errorMsg: 'Did not receive document for ',
                },
                // This rule is not enforced on the java-emulator
                ...(notImplementedInJava || [
                    { rule: 'must be no longer than 1,500 bytes', valid: ['a'.repeat(1500)], invalid: ['a'.repeat(1501)] },
                ]),
                {
                    rule: 'cannot contain a forward slash (/)',
                    valid: ['canContain\\', 'canContain\\/collection/myDoc'],
                    invalid: ['/', 'only/one'],
                    errorMsg: 'Your path does not contain an even number of components.',
                    sync: true,
                },
                {
                    rule: 'cannot solely consist of a single period (.) or double periods (..)',
                    valid: ['.a.', 'a.', '..a', 'a..'],
                    invalid: ['.', '..'],
                    errorMsg: '3 INVALID_ARGUMENT',
                },
                {
                    // in practice .* seems to be more like .+
                    rule: 'cannot match the regular expression __.*__',
                    valid: ['__foo_', '_bar__', '_'.repeat(4)],
                    invalid: ['__foo__', '_'.repeat(5)],
                    errorMsg: '3 INVALID_ARGUMENT',
                },
            ])('$rule', ({ valid, invalid, errorMsg, sync }) => {
                test.each(valid.map(valid => ({ key: valid, description: describe(valid) })))('valid: $description', async ({ key }) => {
                    const ref = fs.collection.doc(key);
                    await ref.create(writeData({ key }));
                    expect(await getDoc(ref)).toEqual({ key });
                });

                test.each(invalid.map(invalid => ({ key: invalid, description: describe(invalid) })))(
                    'invalid: $description',
                    async ({ key }) => {
                        if (sync) {
                            expect(() => fs.collection.doc(key).get()).toThrow(errorMsg);
                        } else {
                            await expect(fs.collection.doc(key).get()).rejects.toThrow(errorMsg);
                        }
                    },
                );

                function describe(str: string) {
                    if (str.length < 15) return str;
                    return `${str.substring(0, 15)}, length: ${str.length}`;
                }
            });
        });

        describe('timestampValue bounds', () => {
            type RawTimestampValue = NonNullable<RawProtoValue['timestampValue']>;

            async function commitRawWrite(write: RawCommitRequest['writes'][number], label: string): Promise<void> {
                const database = (fs.firestore as unknown as { formattedName: string }).formattedName;
                const req: RawCommitRequest = {
                    database,
                    writes: [write],
                };
                await callRawUnaryRpc<RawCommitRequest>(fs.firestore, 'commit', req, label);
            }

            async function commitTimestamp(ref: DocumentReference, timestampValue: RawTimestampValue, label: string): Promise<void> {
                const database = (fs.firestore as unknown as { formattedName: string }).formattedName;
                await commitRawWrite(
                    {
                        update: {
                            name: `${database}/documents/${ref.path}`,
                            fields: {
                                ts: { timestampValue },
                                ttl: rawTtlValue(),
                            },
                        },
                    },
                    label,
                );
            }

            test.each([
                { label: 'minimum boundary (0001-01-01T00:00:00Z)', seconds: -62_135_596_800, nanos: 0 },
                { label: 'maximum microsecond boundary (9999-12-31T23:59:59.999999Z)', seconds: 253_402_300_799, nanos: 999_999_000 },
            ])('accepts $label', async ({ label, seconds, nanos }) => {
                await commitTimestamp(docRef, { seconds, nanos }, `ts-valid-${label}`);
                expect(await getDoc(docRef)).toEqual({ ts: new Timestamp(seconds, nanos) });
            });

            test('accepts maximum nanosecond boundary (9999-12-31T23:59:59.999999999Z)', async () => {
                await commitTimestamp(docRef, { seconds: 253_402_300_799, nanos: 999_999_999 }, 'ts-valid-maximum-nanosecond-boundary');
                const snap = await docRef.get();
                expect(snap.exists).toBeTrue();
                const { ts } = readData<{ ts: Timestamp }>(snap.data());
                expect(ts).toBeInstanceOf(Timestamp);
                expect(ts.seconds).toBe(253_402_300_799);
                expect(ts.nanoseconds).toBeGreaterThanOrEqual(999_999_000);
                expect(ts.nanoseconds).toBeLessThanOrEqual(999_999_999);
            });

            notImplementedInRust ||
                test('truncates sub-microsecond nanoseconds to microsecond precision on write', async () => {
                    await commitTimestamp(docRef, { seconds: 253_402_300_799, nanos: 999_999_999 }, 'ts-truncate-submicrosecond');
                    expect(await getDoc(docRef)).toEqual({ ts: new Timestamp(253_402_300_799, 999_999_000) });
                });

            const invalidTimestamps = [
                { label: 'seconds below minimum (-62135596801)', seconds: -62_135_596_801, nanos: 0 },
                { label: 'seconds above maximum (253402300800)', seconds: 253_402_300_800, nanos: 0 },
                { label: 'negative nanos (-1)', seconds: 0, nanos: -1 },
                { label: 'nanos equal to 1_000_000_000', seconds: 0, nanos: 1_000_000_000 },
            ] as const;

            test.each(invalidTimestamps)('rejects $label with 3 INVALID_ARGUMENT', async ({ label, seconds, nanos }) => {
                await expectRpcCode(() => commitTimestamp(docRef, { seconds, nanos }, `ts-invalid-${label}`), 3, label);
            });

            describe.each([
                {
                    container: 'nested mapValue',
                    buildWrite: (name: string, timestampValue: RawTimestampValue): RawCommitRequest['writes'][number] => ({
                        update: {
                            name,
                            fields: {
                                nested: { mapValue: { fields: { ts: { timestampValue } } } },
                                ttl: rawTtlValue(),
                            },
                        },
                    }),
                },
                {
                    container: 'arrayValue',
                    buildWrite: (name: string, timestampValue: RawTimestampValue): RawCommitRequest['writes'][number] => ({
                        update: {
                            name,
                            fields: {
                                list: { arrayValue: { values: [{ timestampValue }] } },
                                ttl: rawTtlValue(),
                            },
                        },
                    }),
                },
                {
                    container: 'appendMissingElements transform',
                    buildWrite: (name: string, timestampValue: RawTimestampValue): RawCommitRequest['writes'][number] => ({
                        update: {
                            name,
                            fields: { ttl: rawTtlValue() },
                        },
                        updateTransforms: [
                            {
                                fieldPath: 'arr',
                                appendMissingElements: { values: [{ timestampValue }] },
                            },
                        ],
                    }),
                },
            ])('inside $container', ({ container, buildWrite }) => {
                test.each(invalidTimestamps)('rejects $label with 3 INVALID_ARGUMENT', async ({ label, seconds, nanos }) => {
                    const database = (fs.firestore as unknown as { formattedName: string }).formattedName;
                    const name = `${database}/documents/${docRef.path}`;
                    await expectRpcCode(
                        () => commitRawWrite(buildWrite(name, { seconds, nanos }), `ts-invalid-${container}-${label}`),
                        3,
                        `${container}: ${label}`,
                    );
                });
            });
        });
    });

    async function getDoc(ref = docRef) {
        const snap = await ref.get();
        expect(snap.exists).toBeTrue();
        return readData(snap.data());
    }
});
