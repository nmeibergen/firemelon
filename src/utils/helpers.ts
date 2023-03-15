import {
    CollectionRef,
    Firestore
} from '../types/firestore';

export const checkIdsExistence = async (firestore: Firestore, collectionRef: CollectionRef, ids: string[]) => {
    // Divide the ids array into chunks of 10 for batched queries
    const chunks = ids.reduce((resultArray: string[][], item, index) => {
        const chunkIndex: number = Math.floor(index / 10);

        if (resultArray.length <= chunkIndex) {
            resultArray[chunkIndex] = [];
        }

        resultArray[chunkIndex].push(item);

        return resultArray;
    }, []);


    const existingIds = await Promise.all(chunks.map(async chunk =>
        (await collectionRef.where(firestore.FieldPath.documentId(), 'in', chunk).get()).docs.map(doc => doc.id)
    ))

    return existingIds.flat(Infinity) as string[];
}