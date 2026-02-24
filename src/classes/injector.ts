import { Exception } from './common/errors.js';

export class Injector {
  private readonly services = new Map();

  private static instance: Injector;

  private constructor() {}

  static getInstance() {
    if (!this.instance) {
      this.instance = new this();
    }
    return this.instance;
  }

  register<T extends new (...args: any[]) => any>(type: T, params: ConstructorParameters<T>): InstanceType<T> {
    const service = this.services.get(type);
    if (service) throw new Exception('E409');
    const instance = new type(...params);
    this.services.set(type, instance);
    return instance;
  }

  resolve<T extends new (...args: any[]) => any>(type: T): InstanceType<T> {
    const service = this.services.get(type);
    if (!service) throw new Exception('E408');
    return service;
  }
}
