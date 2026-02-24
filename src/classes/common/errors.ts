const ERROR_TYPES = ['Syntax', 'Semantic', 'ConstraintViolation', 'Internal'];

const ERROR_DEFINITIONS = <const>{
  // Syntax Errors (E1xx)
  E100: 'Unexpected end of query',
  E101: 'Unexpected token type',
  E102: 'Unexpected token value',
  E103: 'Unknown column type',
  E104: 'Syntax error',
  E105: 'Unterminated string literal',
  E106: 'Expected NUMBER or STRING in value list',
  E107: 'AUTOINCREMENT can only be used on an INTEGER PRIMARY KEY column',
  E108: 'PRIMARY KEY columns cannot be nullable',
  E109: 'Column with PRIMARY KEY constraint cannot be explicitly marked as NULL',
  E110: 'Unexpected end of expression',
  E111: 'Unknown operator',
  E112: 'Invalid WHERE clause structure',
  E113: 'Invalid query',

  // Semantic Errors (E2xx)
  E200: 'Table not found',
  E201: 'Table already exists',
  E202: 'Schema not found',
  E203: 'Unknown command',
  E204: 'Operator only supports NULL',
  E205: 'Operator cannot be used with NULL',

  // Constraint Violation Errors (E3xx)
  E300: 'Column cannot be null',
  E301: 'UNIQUE constraint failed',
  E302: 'Duplicate value in same insert',

  // Internal Errors (E4xx)
  E400: 'Page not found in directory',
  E401: 'Data buffer must be PAGE_SIZE bytes',
  E402: 'No victim frame found',
  E403: 'Row not found',
  E404: 'Not enough space to insert row',
  E405: 'PageDirectory overflow',
  E406: 'Invalid page ID',
  E407: 'Database initialization failed',
  E408: 'Service not registered',
  E409: 'Service already registered',
  E410: 'Only one instance of Database may exist',
};

export class Exception extends Error {
  constructor(
    public readonly code: keyof typeof ERROR_DEFINITIONS,
    details?: string | number,
  ) {
    const description = ERROR_DEFINITIONS[code];
    const type = ERROR_TYPES[+code[1] - 1];
    const name = `${type}Error`;
    let message = `[${name}] ${code}: ${description}`;
    if (details) message += ` - ${details}`;
    super(message);
    this.name = name;

    Object.setPrototypeOf(this, Exception.prototype);
  }
}
