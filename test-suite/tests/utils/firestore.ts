import { Firestore, Timestamp } from '@google-cloud/firestore';
import assert from 'assert';
import ms from 'ms';

const projectId = 'firestore-test-406503';
const standard = new Firestore({ projectId });
const enterprise = new Firestore({ projectId, databaseId: 'enterprise' });
const enterprisePessimistic = new Firestore({ projectId, databaseId: 'enterprise-pessimistic' });

export const connection = process.env.FIRESTORE_EMULATOR_HOST
    ? process.env.RUST_EMULATOR
        ? 'RUST EMULATOR'
        : 'JAVA EMULATOR'
    : 'CLOUD FIRESTORE';
export const notImplementedInRust = connection === 'RUST EMULATOR' ? [] : undefined;
export const notImplementedInJava = connection === 'JAVA EMULATOR' ? [] : undefined;

// create a separate collection for each test, use TTL on the field `ttl` to delete every document in the collection group `collection`
// after a set time as a fallback, if the tests did not get the chance to delete itself
const standardCollection = standard.collection('tests').doc().collection('collection');
const enterpriseDoc = enterprise.collection('tests').doc();
const enterprisePessimisticDoc = enterprisePessimistic.collection('tests').doc();

export const standardEdition = {
    description: 'Firestore Standard edition',
    firestore: standard,
    collection: standardCollection,
    enterprise: false,
    concurrencyMode: 'pessimistic',
} as const;

export const enterpriseEdition = {
    description: 'Firestore Enterprise edition',
    firestore: enterprise,
    collection: enterpriseDoc.collection('collection'),
    enterprise: true,
    concurrencyMode: 'optimistic',
} as const;

export const enterpriseEditionPessimistic = {
    description: 'Firestore Enterprise edition (pessimistic concurrency)',
    firestore: enterprisePessimistic,
    collection: enterprisePessimisticDoc.collection('collection'),
    enterprise: true,
    concurrencyMode: 'pessimistic',
} as const;

export const editions = [
    standardEdition,
    ...(notImplementedInRust || notImplementedInJava || [enterpriseEdition, enterpriseEditionPessimistic]),
] as const;

export function writeData<T = Record<string, unknown>>(data: FirebaseFirestore.WithFieldValue<T>): T;
export function writeData(): Record<string, unknown>;
export function writeData(data: Record<string, unknown> = {}) {
    const ttl = Timestamp.fromMillis(Date.now() + ms('1h'));
    return { ...data, ttl };
}
export function readData<T = FirebaseFirestore.DocumentData>(data: FirebaseFirestore.DocumentData | undefined) {
    assert(data, 'expected to receive data');
    // Remove the `ttl` value from the data, we are not interested in that one..
    const { ttl, ...realData } = data;
    expect(ttl).toBeInstanceOf(Timestamp);
    return realData as T;
}
export async function readDataRef<T = FirebaseFirestore.DocumentData>(ref: FirebaseFirestore.DocumentReference) {
    const snap = await ref.get();
    return readData<T>(snap.data());
}

afterAll(async () => await standard.recursiveDelete(standardCollection));
// Note that `recursiveDelete` currently does not work on sub-collections with Firestore Enterprise edition
afterAll(async () => await enterprise.recursiveDelete(enterpriseDoc));
afterAll(async () => await enterprisePessimistic.recursiveDelete(enterprisePessimisticDoc));
