import { DocumentData, DocumentReference, Firestore, Timestamp, WithFieldValue } from '@google-cloud/firestore';
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
    ...(notImplementedInJava || []),
    ...(notImplementedInJava || [enterpriseEdition, enterpriseEditionPessimistic]),
] as const;

if (connection === 'RUST EMULATOR') {
    beforeAll(async () => {
        for (const edition of editions) {
            const databaseId = edition.firestore.databaseId;
            const databaseEdition = edition.enterprise ? 'ENTERPRISE' : 'STANDARD';
            const concurrencyMode = edition.concurrencyMode.toUpperCase();
            const url = `http://${process.env.FIRESTORE_EMULATOR_HOST}/v1/projects/${projectId}/databases/${databaseId}`;
            const res = await fetch(url, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ databaseEdition, concurrencyMode }),
            });
            const body = await res.text();
            if (!res.ok) throw new Error(body);
        }
    });
}

export function writeData<T = Record<string, unknown>>(data: WithFieldValue<T>): T;
export function writeData(): Record<string, unknown>;
export function writeData(data: Record<string, unknown> = {}) {
    const ttl = Timestamp.fromMillis(Date.now() + ms('1h'));
    return { ...data, ttl };
}
export function readData<T = DocumentData>(data: DocumentData | undefined) {
    assert(data, 'expected to receive data');
    // Remove the `ttl` value from the data, we are not interested in that one..
    const { ttl, ...realData } = data;
    expect(ttl).toBeInstanceOf(Timestamp);
    return realData as T;
}
export async function readDataRef<T = DocumentData>(ref: DocumentReference) {
    const snap = await ref.get();
    return readData<T>(snap.data());
}

afterAll(async () => await standard.recursiveDelete(standardCollection));
// Note that `recursiveDelete` currently does not work on sub-collections with Firestore Enterprise edition
afterAll(async () => await enterprise.recursiveDelete(enterpriseDoc));
afterAll(async () => await enterprisePessimistic.recursiveDelete(enterprisePessimisticDoc));
