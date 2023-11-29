import app from 'firebase-admin';
import ms from 'ms';

app.initializeApp({
    projectId: 'firestore-test-406503',
});

const firestore = app.firestore();
export const FirebaseFirestore = app.firestore;

// create a separate collection for each test, use TTL to delete every document after a set time as a fallback, if the tests did not
// get the chance to delete itself
const mainTestDoc = firestore.collection('tests').doc();
export const collection = mainTestDoc.collection('collection');

export function docData(data: object = {}) {
    const ttl = FirebaseFirestore.Timestamp.fromMillis(Date.now() + ms('1h'));
    return { ...data, ttl };
}

afterAll(async () => await firestore.recursiveDelete(collection));
