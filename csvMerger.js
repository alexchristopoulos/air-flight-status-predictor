const fs = require('fs');

var ws = fs.createWriteStream('./merged.csv', {
    encoding: 'utf-8'
});

const dir = fs.readdirSync('./tmp')

dir.forEach((file, index) => {

    var lines = fs.readFileSync('./tmp/' + file).toString().split('\n');
    console.log('file ' + file + ' {' + (parseInt(index) + 1) + '/' + dir.length + '}');

    for (var i = 0; i < lines.length; i++) {

        if (index == 0 && i == 0) {

            if (lines[i].length > 10)
                ws.write(lines[i] + '\n');

        } else if (i > 0) {

            if (lines[i].length > 10) {

                if (parseInt(index) == (parseInt(dir.length) - 1) && parseInt(i) == (parseInt(lines.length) - 2)) {
                    console.log('end');
                    ws.write(lines[i]);
                    break;
                } else {
                    ws.write(lines[i] + '\n');
                }
            }

        } else {

            continue;
        }
    }
});

ws.close();
// node .\mergeCsv.js --max-old-space-size=8192