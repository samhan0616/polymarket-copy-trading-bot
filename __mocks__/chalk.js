// Mock chalk for Jest tests to avoid ESM import issues
const chalk = {
    red: (text) => text,
    green: (text) => text,
    blue: (text) => text,
    yellow: (text) => text,
    cyan: (text) => text,
    magenta: (text) => text,
    white: (text) => text,
    gray: (text) => text,
    grey: (text) => text,
    black: (text) => text,
    bgRed: (text) => text,
    bgGreen: (text) => text,
    bgBlue: (text) => text,
    bgYellow: (text) => text,
    bgCyan: (text) => text,
    bgMagenta: (text) => text,
    bgWhite: (text) => text,
    bgBlack: (text) => text,
    bold: (text) => text,
    dim: (text) => text,
    italic: (text) => text,
    underline: (text) => text,
    inverse: (text) => text,
    hidden: (text) => text,
    strikethrough: (text) => text,
};

// Make all properties chainable
Object.keys(chalk).forEach((key) => {
    const fn = chalk[key];
    Object.keys(chalk).forEach((innerKey) => {
        fn[innerKey] = chalk[innerKey];
    });
});

module.exports = chalk;
module.exports.default = chalk;
