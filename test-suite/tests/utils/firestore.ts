import assert from 'assert';
import app from 'firebase-admin';
import ms from 'ms';

app.initializeApp({
    projectId: 'firestore-test-406503',
});

export const firestore = app.firestore();
export const exported = app.firestore;

export const connection = process.env.FIRESTORE_EMULATOR_HOST
    ? process.env.RUST_EMULATOR
        ? 'RUST EMULATOR'
        : 'JAVA EMULATOR'
    : 'CLOUD FIRESTORE';
export const notImplementedInRust = connection === 'RUST EMULATOR' ? [] : undefined;
export const notImplementedInJava = connection === 'JAVA EMULATOR' ? [] : undefined;
export const notImplementedInCloud = connection === 'CLOUD FIRESTORE' ? [] : undefined;

// create a separate collection for each test, use TTL on the field `ttl` to delete every document in the collection group `collection`
// after a set time as a fallback, if the tests did not get the chance to delete itself
const mainTestDoc = firestore.collection('tests').doc();
export const collection = mainTestDoc.collection('collection');

export function writeData<T = Record<string, unknown>>(data: FirebaseFirestore.WithFieldValue<T>): T;
export function writeData(): Record<string, unknown>;
export function writeData(data: Record<string, unknown> = {}) {
    const ttl = exported.Timestamp.fromMillis(Date.now() + ms('1h'));
    return { ...data, ttl };
}
export function readData<T = FirebaseFirestore.DocumentData>(data: FirebaseFirestore.DocumentData | undefined) {
    assert(data, 'expected to receive data');
    // Remove the `ttl` value from the data, we are not interested in that one..
    const { ttl, ...realData } = data;
    expect(ttl).toBeInstanceOf(exported.Timestamp);
    return realData as T;
}
export async function readDataRef<T = FirebaseFirestore.DocumentData>(ref: FirebaseFirestore.DocumentReference) {
    const snap = await ref.get();
    return readData<T>(snap.data());
}

afterAll(async () => await firestore.recursiveDelete(collection));
