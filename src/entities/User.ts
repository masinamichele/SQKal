import { EntityModel } from '../classes/decorators/entity-model.decorator.js';
import { Entity } from '../classes/entity.js';
import { Column } from '../classes/decorators/column.decorator.js';

@EntityModel('users')
export class User extends Entity {
  @Column('number')
  id: number;

  @Column('string')
  name: string;
}
