// The best way to run this is:
// make compile_js
//

// const webpack = require('webpack');
// const path = require('path');

module.exports = {
  entry: {
    // app: './assets/js/app.jsx'  // overriden by app=assets/js/app.jsx in Makefile
  },
  output: {
    filename: '[name].js',
    // path: path.resolve(__dirname, 'dist') // overriden by --output-path in Makefile
  },
  resolve: {
    modules: [ '.', 'node_modules' ]
  },
  externals: {
    'react': 'React',
    'react-dom': 'ReactDOM',
    'react-router': 'ReactRouter',
    'react-router-dom': 'ReactRouterDOM',
    'react-bootstrap': 'ReactBootstrap',
    'react-router-bootstrap': 'ReactRouterBootstrap',
    'prop-types': 'PropTypes',
    'codemirror': 'CodeMirror',
    'axios': 'axios',
    'react-bootstrap-table': 'ReactBootstrapTable',
    'vis': 'vis'
  },
  module: {
    rules: [
      {
        test: /\.jsx?$/,
        use: {
          loader: 'babel-loader',
          options: {
            presets: [ "es2015", "react" ]
          }
        }
      }
    ]
  }
};
