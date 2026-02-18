const store = new WeakMap<object, Map<any, any>>();

export class ReflectMetadata {
  private constructor() {}

  static define<T>(metadataKey: any, metadataValue: T, target: object) {
    if (!store.has(target)) {
      store.set(target, new Map<any, any>());
    }
    const targetMetadata = store.get(target);
    targetMetadata.set(metadataKey, metadataValue);
  }

  static get<T = any>(metadataKey: any, target: object): T {
    const targetMetadata = store.get(target);
    if (targetMetadata) return targetMetadata.get(metadataKey) as T;
    return undefined;
  }
}
