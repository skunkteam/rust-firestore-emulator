import { FirebaseFirestore, collection, docData } from './utils/firestore';

let docRef: FirebaseFirestore.DocumentReference;
beforeEach(async () => {
    docRef = collection.doc();
});

test('getting a non-existing document', async () => {
    const snap = await docRef.get();
    expect(snap.exists).toBeFalse();
    expect(snap.data()).toBeUndefined();
});

describe('creating', () => {
    describe.each([
        { using: 'set', createFn: (data: Record<string, unknown>) => docRef.set(data) },
        { using: 'create', createFn: (data: Record<string, unknown>) => docRef.create(data) },
    ] as const)('using $using', ({ createFn }) => {
        test('setting and getting a basic document', async () => {
            const data = docData({ foo: 'bar' });
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
            await createFn(docData(data));

            expect(await getDoc()).toEqual(data);
        });

        test('using serverTimestamp', async () => {
            await createFn(
                docData({
                    string: 'foo',
                    firstTime: FirebaseFirestore.FieldValue.serverTimestamp(),
                    object: {
                        withSecondTime: FirebaseFirestore.FieldValue.serverTimestamp(),
                    },
                }),
            );

            const doc = await getDoc();

            expect(doc).toEqual({
                string: 'foo',
                firstTime: expect.any(FirebaseFirestore.Timestamp),
                object: {
                    withSecondTime: expect.any(FirebaseFirestore.Timestamp),
                },
            });
            // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
            expect(doc?.firstTime).toEqual(doc?.object.withSecondTime); // should be equal including nanoseconds
        });

        test('no serverTimestamp in an Array', async () => {
            const data = docData({ arr: [FirebaseFirestore.FieldValue.serverTimestamp()] });
            // Is checked client side apparently
            expect(() => createFn(data)).toThrow('FieldValue.serverTimestamp() cannot be used inside of an array');
        });
    });

    test('can only create once', async () => {
        await docRef.create(docData());
        await expect(docRef.create(docData())).rejects.toThrow('6 ALREADY_EXISTS');
    });
});

describe('updating', () => {
    const data = docData();

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
                    await expect(updateFn({ newField: FirebaseFirestore.FieldValue.delete() })).rejects.toThrow(
                        'FieldValue.delete() must appear at the top-level and can only be used in update() or set() with {merge:true}',
                    );
                } else {
                    await updateFn({ newField: FirebaseFirestore.FieldValue.delete() });
                    expect(await getDoc()).not.toHaveProperty('newField');
                }
            });

            test('increment', async () => {
                // `increment` on a non existing value sets the value
                await updateFn({ counter: FirebaseFirestore.FieldValue.increment(1) });
                expect(await getDoc()).toEqual({ counter: 1 });

                // When using `set` without `merge`, the `increment` operation will never 'merge' the field, it will just set it to the
                // increment value.
                await updateFn({ counter: FirebaseFirestore.FieldValue.increment(15) });
                expect(await getDoc()).toEqual({ counter: setOperation ? 15 : 16 });

                await updateFn({ counter: FirebaseFirestore.FieldValue.increment(-32.1) });
                expect(await getDoc()).toEqual({ counter: setOperation ? -32.1 : -16.1 });
            });

            test('arrayUnion', async () => {
                await updateFn({ arr: FirebaseFirestore.FieldValue.arrayUnion({ first: 'value' }, 'second') });
                expect(await getDoc()).toEqual({ arr: [{ first: 'value' }, 'second'] });

                // It should deduplicate `first`, but add `third`
                await updateFn({ arr: FirebaseFirestore.FieldValue.arrayUnion({ third: 3 }, { first: 'value' }) });
                if (setOperation) {
                    expect(await getDoc()).toEqual({ arr: [{ third: 3 }, { first: 'value' }] });
                } else {
                    expect(await getDoc()).toEqual({ arr: [{ first: 'value' }, 'second', { third: 3 }] });
                }
            });

            test('arrayRemove', async () => {
                await updateFn({ arr: [{ first: 'foo' }, { second: 'bar' }, 'third', 'third'] });

                await updateFn({ arr: FirebaseFirestore.FieldValue.arrayRemove({ doesNot: 'exist' }) });
                expect(await getDoc()).toEqual({ arr: setOperation ? [] : [{ first: 'foo' }, { second: 'bar' }, 'third', 'third'] });

                await updateFn({ arr: FirebaseFirestore.FieldValue.arrayRemove('third', { first: 'foo' }, 'nope') });
                expect(await getDoc()).toEqual({ arr: setOperation ? [] : [{ second: 'bar' }] });
            });
        });
    });
});

describe('deleting', () => {
    test('existing document', async () => {
        await docRef.set(docData());
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
            ...(process.env.FIRESTORE_EMULATOR_HOST
                ? []
                : [{ rule: 'must be no longer than 1,500 bytes', valid: ['a'.repeat(1500)], invalid: ['a'.repeat(1501)] }]),
            {
                rule: 'cannot contain a forward slash (/)',
                valid: ['canContain\\', 'canContain/collection/myDoc'],
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
                const ref = collection.doc(key);
                await ref.create(docData({ key }));
                expect(await getDoc(ref)).toEqual({ key });
            });

            test.each(invalid.map(invalid => ({ key: invalid, description: describe(invalid) })))(
                'invalid: $description',
                async ({ key }) => {
                    if (sync) {
                        expect(() => collection.doc(key).get()).toThrow(errorMsg);
                    } else {
                        await expect(collection.doc(key).get()).rejects.toThrow(errorMsg);
                    }
                },
            );

            function describe(str: string) {
                if (str.length < 15) return str;
                return `${str.substring(0, 15)}, length: ${str.length}`;
            }
        });
    });
});

async function getDoc(ref = docRef) {
    const snap = await ref.get();
    expect(snap.exists).toBeTrue();
    // Remove the `ttl` value from the data, we are not interested in that one..
    const { ttl, ...realData } = snap.data()!;
    expect(ttl).toBeInstanceOf(FirebaseFirestore.Timestamp);
    return realData;
}
