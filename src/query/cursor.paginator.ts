import {
  Brackets,
  ObjectType,
  OrderByCondition,
  SelectQueryBuilder,
  WhereExpressionBuilder,
} from 'typeorm';

const msgpack = require('msgpack5')()
import { CursorPageInfo, CursorPaging, CursorResult, SortDirection } from '@nest-query/api';






export interface PaginationOptions<Entity> {
  entity: ObjectType<Entity>;
  alias?: string;
  paging?: CursorPaging;
  paginationKeys?: Extract<keyof Entity, string>[];
}

export function buildPaginator<Entity>(options: PaginationOptions<Entity>): Paginator<Entity> {
  const {
    entity,
    paging = {},
    alias = entity.name.toLowerCase(),
    paginationKeys = ['id' as any],
  } = options;

  const paginator = new Paginator(entity, paginationKeys);

  paginator.setAlias(alias);

  if (paging.after) {
    paginator.setAfterCursor(paging.after);
  }

  if (paging.before) {
    paginator.setBeforeCursor(paging.before);
  }

  if (paging.limit) {
    paginator.setLimit(paging.limit);
  }

  if (paging.order) {
    paginator.setOrder(paging.order as SortDirection);
  }

  return paginator;
}





export type EscapeFn = (name: string) => string;

export interface CursorParam {
  [key: string]: any;
}

export function camelOrPascalToUnderscore(str: string): string {
  return str.split(/(?=[A-Z])/).join('_').toLowerCase();
}

export function pascalToUnderscore(str: string): string {
  return camelOrPascalToUnderscore(str);
}

export default class Paginator<Entity> {
  private afterCursor: string | null = null;

  private beforeCursor: string | null = null;

  private nextAfterCursor: string | null = null;

  private nextBeforeCursor: string | null = null;

  private alias: string = pascalToUnderscore(this.entity.name);

  private limit = 100;

  private order: SortDirection = SortDirection.DESC;

  public constructor(
    private entity: ObjectType<Entity>,
    private paginationKeys: Extract<keyof Entity, string>[],
  ) {}

  public setAlias(alias: string): void {
    this.alias = alias;
  }

  public setAfterCursor(cursor: string): void {
    this.afterCursor = cursor;
  }

  public setBeforeCursor(cursor: string): void {
    this.beforeCursor = cursor;
  }

  public setLimit(limit: number): void {
    this.limit = limit;
  }

  public setOrder(order: SortDirection): void {
    this.order = order;
  }

  public async paginate(
    builder: SelectQueryBuilder<Entity>,
  ): Promise<CursorResult<Entity>> {
    const entities = await this.appendPagingQuery(builder).getMany();
    const hasMore = entities.length > this.limit;

    if (hasMore) {
      entities.splice(entities.length - 1, 1);
    }

    if (entities.length === 0) {
      return this.toPagingResult(entities);
    }

    if (!this.hasAfterCursor() && this.hasBeforeCursor()) {
      entities.reverse();
    }

    if (this.hasBeforeCursor() || hasMore) {
      this.nextAfterCursor = this.encode(entities[entities.length - 1]);
    }

    if (this.hasAfterCursor() || (hasMore && this.hasBeforeCursor())) {
      this.nextBeforeCursor = this.encode(entities[0]);
    }

    return this.toPagingResult(entities);
  }

  private getCursorPageInfo(): CursorPageInfo {
    return {
      nextCursor: this.nextAfterCursor,
      previousCursor: this.nextBeforeCursor,
    };
  }

  private appendPagingQuery(
    builder: SelectQueryBuilder<Entity>,
  ): SelectQueryBuilder<Entity> {
    const cursors: CursorParam = {};
    const clonedBuilder = new SelectQueryBuilder<Entity>(builder)

    if (this.hasAfterCursor()) {
      Object.assign(cursors, this.decode(this.afterCursor as string));
    } else if (this.hasBeforeCursor()) {
      Object.assign(cursors, this.decode(this.beforeCursor as string));
    }

    if (Object.keys(cursors).length > 0) {
      clonedBuilder.andWhere(
        new Brackets((where) => this.buildCursorQuery(where, cursors)),
      );
    }

    clonedBuilder.take(this.limit + 1);
    clonedBuilder.orderBy(this.buildOrder());

    return clonedBuilder;
  }

  private buildCursorQuery(
    where: WhereExpressionBuilder,
    cursors: CursorParam,
  ): void {
    const operator = this.getOperator();
    const params: CursorParam = {};
    
    const keys: Extract<keyof Entity, string>[] = [];

    this.paginationKeys.forEach((key) => {
      params[key] = cursors[key];
      
      where.orWhere(new Brackets((qb) => {
        keys.forEach(it => {
          const paramsHolder = {
            [`${it}_1`]: params[it],
            [`${it}_2`]: params[key],
          };
          qb.andWhere(`${this.alias}.${it} = :${it}_1`, paramsHolder);  
        })

        const paramsHolder = {
          [`${key}_1`]: params[key],
          [`${key}_2`]: params[key],
        };
        qb.andWhere(`${this.alias}.${key} ${operator} :${key}_1`, paramsHolder);
      }));

      keys.push(key);
    });
  }

  private getOperator(): string {
    if (this.hasAfterCursor()) {
      return this.order === SortDirection.ASC ? '>' : '<';
    }

    if (this.hasBeforeCursor()) {
      return this.order === SortDirection.ASC ? '<' : '>';
    }

    return '=';
  }

  private buildOrder(): OrderByCondition {
    let { order } = this;

    if (!this.hasAfterCursor() && this.hasBeforeCursor()) {
      order = this.flipOrder(order);
    }

    const orderByCondition: OrderByCondition = {};
    this.paginationKeys.forEach((key) => {
      orderByCondition[`${this.alias}.${key}`] = order;
    });

    return orderByCondition;
  }

  private hasAfterCursor(): boolean {
    return this.afterCursor !== null;
  }

  private hasBeforeCursor(): boolean {
    return this.beforeCursor !== null;
  }

  private encode(entity: Entity): string {
    const payload = {} as Record<string, any>;
    this.paginationKeys.forEach((k) => { payload[k] = entity[k] });
    return msgpack.encode(payload).toString('hex');
  }

  private decode(str: string): CursorParam {
    const buf = Buffer.from(str, 'hex');
    return msgpack.decode(buf);
  }

  private flipOrder(order: SortDirection): SortDirection {
    return order === SortDirection.ASC ? SortDirection.DESC : SortDirection.ASC;
  }

  private toPagingResult<Entity>(entities: Entity[]): CursorResult<Entity> {
    return {
      data: entities,
      pageInfo: this.getCursorPageInfo(),
    };
  }
}