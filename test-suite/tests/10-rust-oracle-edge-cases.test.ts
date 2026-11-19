import { DocumentReference, Timestamp } from '@google-cloud/firestore';
import { editions, writeData } from './utils';

describe.each(editions)('$description - precondition edge cases', fs => {
    let docRef: DocumentReference;
    beforeEach(async () => {
        docRef = fs.collection.doc();
    });

    test('updating a non-existing document rejects with 5 NOT_FOUND', async () => {
        await expect(docRef.update({ foo: 'bar' })).rejects.toThrow('5 NOT_FOUND');
    });

    test('deleting a non-existing document with exists=true rejects with 5 NOT_FOUND', async () => {
        await expect(docRef.delete({ exists: true })).rejects.toThrow('5 NOT_FOUND');
    });

    test('updating or deleting an existing document with mismatched lastUpdateTime rejects with 9 FAILED_PRECONDITION', async () => {
        await docRef.set(writeData({ foo: 'initial' }));
        const wrongTime = Timestamp.fromMillis(1_000_000);

        await expect(docRef.update({ foo: 'updated' }, { lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
        await expect(docRef.delete({ lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
    });

    test('updating or deleting a non-existing document with lastUpdateTime rejects with 9 FAILED_PRECONDITION', async () => {
        const wrongTime = Timestamp.fromMillis(1_000_000);

        await expect(docRef.update({ foo: 'updated' }, { lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
        await expect(docRef.delete({ lastUpdateTime: wrongTime })).rejects.toThrow('9 FAILED_PRECONDITION');
    });

    test('updating or deleting a deleted document with its delete writeTime rejects with 9 FAILED_PRECONDITION', async () => {
        await docRef.set(writeData({ foo: 'initial' }));
        const { writeTime: deleteTime } = await docRef.delete();

        await expect(docRef.update({ foo: 'after-delete' }, { lastUpdateTime: deleteTime })).rejects.toThrow('9 FAILED_PRECONDITION');
        await expect(docRef.delete({ lastUpdateTime: deleteTime })).rejects.toThrow('9 FAILED_PRECONDITION');
    });

    test('failed multi-write batch leaves earlier document untouched', async () => {
        const missingRef = fs.collection.doc();
        const batch = fs.firestore.batch();
        batch.set(docRef, writeData({ committed: true }));
        batch.update(missingRef, { missing: true });

        await expect(batch.commit()).rejects.toThrow('5 NOT_FOUND');
        expect(await docRef.get()).toHaveProperty('exists', false);
    });
});
