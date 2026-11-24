export const connection = process.env.FIRESTORE_EMULATOR_HOST
    ? process.env.RUST_EMULATOR
        ? 'RUST EMULATOR'
        : 'JAVA EMULATOR'
    : 'CLOUD FIRESTORE';
export const notImplementedInRust = connection === 'RUST EMULATOR' ? [] : undefined;
export const notImplementedInJava = connection === 'JAVA EMULATOR' ? [] : undefined;
