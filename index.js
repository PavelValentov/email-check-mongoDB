// checking emails in a MongoDB base 'emails'
//
// STARTING SYNTAX:
// NPM START
// or
// NPM START MONGODB_HOST_IP
//
// while executing press Escape to finish process

const mongoClient = require('mongodb').MongoClient;
const emailCheck = require('email-check');
const events = require('events');

const readline = require('readline');
readline.emitKeypressEvents(process.stdin);

const emitter = new events.EventEmitter();

// check for params
const dbParams = require('./config/myconfig.json');
if (!dbParams) {
  console.log('ERROR: Check config.json params');
  return;
}

// form the connection string
// like this
// const connectStr = 'mongodb://user:pass@127.0.0.1:27017/emails';
let connDBStr = 'mongodb://';
if (dbParams.user) {
  connDBStr += dbParams.user;
  if (dbParams.pass) {
    connDBStr += ':' + dbParams.pass;
  }
  connDBStr += '@';
}
connDBStr += dbParams.host;
if (dbParams.port) {
  connDBStr += ':' + dbParams.port;
}
if (dbParams.db) {
  connDBStr += '/' + dbParams.db;
}

let saveDBTimeout,
  finishingTimeoutID,
  checkIntervalID,
  updateIndervalID;

let task = {
  started: false,
  savingDB: false,
  emails: [],
  pos: -1, // active position in the emails[]
  table: 'wildberries',
  mailOptions: {
    // from: 'search@kremlin.gov',
    timeout: 3000
  },
  emailDomains: [],
  skipDomains: [ // skip these emails
    '@fsb',
    '.gov',
    'kremlin.ru',
    '.ua',
    '.kz',
    'icloud.com'
  ],
  limitRecords: 500000, // number of records to get from DB
  limitTasksNumber: 32, // number of check tasks at the same time
  limitTasksTime: 15000, // time limit (Msec) for checking one email
  // delayAfterSaveDB: 120000, // delay after save a buffer to DB (for safe DB updates)
  delayEachRecord: 800, // delay Msec after each email iteration
  breakAfterTimeouts: 500, // break task after X timeout errors
  checkQueue: [], // email checkQueue
  updateQueue: [], // records queue to save into DB
  updateQueuePos: 0, // count records saved into DB (position + 1 in the updateQueue)
  limitSaveBufferSize: 500, // records number of buffer size to save into DB
  breakCounter: 0, // break the task after x records
  stat: {
    good: 0,
    bad: 0,
    skip: 0,
    refused: 0,
    timeout: 0,
    error: 0
  }
};

process.stdin.setRawMode(true);
process.stdin.resume();
process.stdin.on('keypress', function (chunk, key) {
  if (key && key.ctrl && key.name === 'c') {
    process.stdout.write('BREAKED. Waiting for 5 seconds...\n');
    finishTask();
    setTimeout(() => {
      process.exit();
    }, 5000);
  }
  else if (key && key.name === 'escape') {
    // process.stdout.write('FINISHING...\n');
    finishTask();
  }
});

emitter.on('done', (res) => {
  task.updateQueue.push(res);
  clearTasks(res.email);
  if (task.started === true) {
    setTimeout(() => {
      checkArrayTime();
    }, task.delayEachRecord);
  }
});

emitter.on('skip', (email) => {
  // console.log('SKIP ' + email);
  task.stat.skip++;
  emitter.emit('done', {
    result: 'false',
    lastCheck: new Date(),
    email: email,
    reason: 'SKIP'
  });
});

emitter.on('saveDB', () => {
  saveDB();
});

emitter.on('savingDB', (status) => {
  if (!task.savingDB && saveDBTimeout) {
    clearTimeout(saveDBTimeout);
    saveDBTimeout = undefined;
  }
  task.savingDB = status;
});

emitter.on('start', (email) => {
  if (email) {
    // console.log((task.pos + 1) + ' [' + task.checkQueue.length + '] checking the mail: ' + email);

    const taskItem = {
      email: email,
      start: new Date().getTime()
    };
    task.checkQueue.push(taskItem);

    setImmediate(() => {
      emailCheck(email, task.mailOptions)
        .then(function (res) {
          // console.log(res + ': ' + email);
          if (res === true) task.stat.good++;
          else task.stat.bad++;
          emitter.emit('done', {
            result: res,
            lastCheck: new Date(),
            email: email,
            reason: null
          });
        })
        .catch((err) => {
          if (err.message === 'refuse') {
            // console.log('FALSE: ' + email + ' - MX server is refusing your IP');
            task.stat.refused++;
            emitter.emit('done', {
              result: false,
              lastCheck: new Date(),
              email: email,
              reason: 'refuse'
            });
            // console.log('Error: ' + (err.message === 'refuse') ? 'The MX server is refusing requests from your IP address' : '');
          }
          else {
            console.log('ERROR: ' + email + ' - ' + err.message);
            task.stat.error++;
            emitter.emit('done', {
              result: false,
              lastCheck: new Date(),
              email: email,
              reason: err.message
            });
          }
        });
    });
  } else {
    task.stat.error++;
    // console.log('EMPTY EMAIL');
    if (task.started) {
      setTimeout(() => {
        checkArrayTime();
      }, task.delayEachRecord);
    }
  }
});

function updateDomain(email, reason) { // add domains to array
  if (!email) return false;

  let div = email.indexOf('@');
  if (div > -1) {
    let domain = email.substr(div + 1);
    let elPos = task.emailDomains.findIndex((elm, idx, arr) => {
      if (elm.domain === domain) {
        return true
      }
    });
    if (elPos === -1) {
      elPos = task.emailDomains.push({
        domain: domain
      }) - 1;
    }
    if (reason) {
      if (reason.toLowerCase() === 'skip') {
        if (task.emailDomains[elPos].hasOwnProperty('skip')) task.emailDomains[elPos].skip++;
        else task.emailDomains[elPos].skip = 1;

        if (task.skipDomains.indexOf(domain) === -1) task.skipDomains.push(domain);
      }
      if (reason.toLowerCase() === 'timeout') {
        if (task.emailDomains[elPos].hasOwnProperty('timeout')) task.emailDomains[elPos].timeout++;
        else task.emailDomains[elPos].timeout = 1;

        if (task.emailDomains[elPos].timeout > 10) {
          if (task.skipDomains.indexOf(domain) === -1) task.skipDomains.push(domain);
        }
      }
      if (reason.toLowerCase() === 'timeout') {
        if (task.emailDomains[elPos].hasOwnProperty('timeout')) task.emailDomains[elPos].timeout++;
        else task.emailDomains[elPos].timeout = 1;

        if (task.emailDomains[elPos].timeout > 10) {
          if (task.skipDomains.indexOf(domain) === -1) task.skipDomains.push(domain);
        }
      }
    }
  } else return false;
}

function saveDB() {
  if (task.savingDB === true) return;
  emitter.emit('savingDB', true);

  let arr,
    bulk = [],
    record,
    bufSize = task.updateQueue.length - task.updateQueuePos > task.limitSaveBufferSize ? task.limitSaveBufferSize : task.updateQueue.length - task.updateQueuePos;

  arr = task.updateQueue.slice(task.updateQueuePos, task.updateQueuePos + bufSize);
  if (!(arr.length > 0)) {
    emitter.emit('savingDB', false);
    return;
  }

  for (let i = 0; i < arr.length; i++) {
    record = arr[i];

    if (!record.email) {
      console.log('ERROR in arr...');
      console.log(record);
      continue;
    }

    bulk.push({
      "updateMany": {
        "filter": {"email": record.email},
        "update": {
          $set: {
            checkEmail: {
              result: record.result,
              lastCheck: record.lastCheck,
              email: record.email,
              reason: record.reason
            }
          }
        }
      }
    });
  }

  if (!(bulk.length > 0)) {
    emitter.emit('savingDB', false);
    return;
  }

  task.updateQueuePos += bufSize;
  // console.log('Saving buffer to DB: ' + bufSize + ' records');

  mongoClient.connect(connDBStr)
    .then((db) => {
      db.collection(task.table).bulkWrite(bulk, {'ordered': true, 'w': 1})
        .then((res) => {
          console.log('SAVED ' + res.matchedCount + '[' + task.updateQueuePos + '/' + task.updateQueue.length + '] records');
          db.close();
          emitter.emit('savingDB', false);
        })
        .catch((err) => {
          console.log('DB ERROR: ' + err.message);
          db.close();
          emitter.emit('savingDB', false);
        });

      // just waiting DB response
      // saveDBTimeout = setTimeout(() => {
      //   emitter.emit('savingDB', false);
      //   console.log('Going on...')
      // }, task.delayAfterSaveDB);
    })
    .catch((err) => {
      console.log('Can not connect to DB: ' + err.message);
      emitter.emit('savingDB', false);
      process.exit();
    });
}

function needSkipEmail(email) {
  let result = false;
  for (let item in task.skipDomains) {
    if (email.indexOf(task.skipDomains[item]) > -1) {
      result = true;
      break;
    }
  }
  return result;
}

function clearTasks(email) {
  let now;
  for (let i = task.checkQueue.length - 1; i >= 0; i--) {
    if ((typeof email !== 'undefined') // delete one email
      && task.checkQueue.length >= i + 1
      && task.checkQueue[i].hasOwnProperty('email')
      && task.checkQueue[i].email === email) {

      // console.log('OUT ' + email);
      task.checkQueue.splice(i, 1);
      break;
    } else { // delete TIMEOUTS
      now = new Date().getTime();
      if (task.checkQueue.length >= i + 1
        && task.checkQueue[i].hasOwnProperty('start')
        && task.checkQueue[i].hasOwnProperty('email')
        && now - task.checkQueue[i].start > task.limitTasksTime) {

        console.log('TIMEOUT ' + task.checkQueue[i].email);
        task.stat.timeout++;

        emitter.emit('done', {
          result: 'false',
          lastCheck: new Date(),
          email: task.checkQueue[i].email,
          reason: 'TIMEOUT'
        });
        task.checkQueue.splice(i, 1);
      }
    }
  }
}

function finishTask() {
  if (task.started === true) {
    task.started = false;
    console.log('FINISHING...');
  }

  if (finishingTimeoutID) return;

  if (task.checkQueue.length > 0) {
    console.log('WAITING for ' + task.checkQueue.length + ' records...');
    setImmediate(() => {
      clearTasks();
    });
    finishingTimeoutID = setTimeout(() => {
      finishTask();
      clearTimeout(finishingTimeoutID);
      finishingTimeoutID = undefined;
    }, 1000);
  }
}

function checkArrayTime() {
  // if (task.savingDB === true) return;

  if (task.stat.timeout > task.breakAfterTimeouts) {
    console.log('Too much TIMEOUTS [' + task.stat.timeout + ']');
    finishTask();
    return;
  }

  if (task.pos >= task.emails.length - 1) {
    finishTask();
    return;
  }

  if ((task.breakCounter > 0) && (task.pos + 1 >= task.breakCounter)) {
    finishTask();
    return;
  }

  if (task.checkQueue.length >= task.limitTasksNumber) {
    setImmediate(() => {
      clearTasks();
    });
    return;
  }

  task.pos++;
  let email = task.emails[task.pos].email;

  if (needSkipEmail(email) === true) {
    emitter.emit('skip', email);
    return;
  }

  emitter.emit('start', email);
}

function executeEmailsArray() {
  task.started = true;
  checkIntervalID = setInterval(() => {
      if (task.started) {
        const total = task.stat.good + task.stat.bad + task.stat.error +
          task.stat.skip + task.stat.refused + task.stat.timeout;

        console.log(
          'Check#: ' + (task.pos + 1) +
          ' [THREADS:' + task.checkQueue.length + ']' +
          ' GOOD:' + task.stat.good +
          ' BAD:' + task.stat.bad +
          ' ERROR:' + task.stat.error +
          ' SKIP:' + task.stat.skip +
          ' REFUSED:' + task.stat.refused +
          ' TIMEOUT:' + task.stat.timeout +
          ' TOTAL:' + total
        );

        clearTasks();

        checkArrayTime();
      } else {
        finishTask();
        clearInterval(checkIntervalID);
        checkIntervalID = undefined;
      }
    }
    , 1000);

  updateIndervalID = setInterval(() => {
    if ((task.updateQueue.length - task.updateQueuePos <= 0) && !task.started && !task.savingDB) {
      clearInterval(updateIndervalID);
      updateIndervalID = undefined;
      console.log('WORK DONE: ' + new Date());
      process.exit(0);
    }

    if (!task.savingDB) {
      let bufferSize = task.updateQueue.length - task.updateQueuePos;
      if ((bufferSize >= task.limitSaveBufferSize) || (bufferSize > 0 && !task.started)) {
        emitter.emit('saveDB');
      }
    }
  }, 1000);
}

mongoClient.connect(connDBStr)
  .then((db) => {
    db.collection(task.table).distinct(
      "email",
      // {$and: [{"email": {$ne: null}}, {"email": {$ne: ''}}, {"checkEmail": null}]}
      // {$and: [{"email": {$regex: /^[a-e,0-9][a-z,0-9].*/i}}, {"checkEmail": null}]}
      // {$and: [{"email": {$regex: /^[a,0-9][a,0-9].*/i}}, {"checkEmail": null}]}
      {$and: [{"email": {$regex: /^[c-j].*/i}}, {"checkEmail": null}]}
    )
      .then((arr) => {
        if (arr.length > 0) {
          const needRecords = arr.length > task.limitRecords ? task.limitRecords : arr.length;
          console.log('Copying ' + needRecords + ' records to the memory...');
          for (let i = 0; i < needRecords; i++) {
            task.emails.push({email: arr[i]});
          }
          console.log('Copying DONE');
          db.close();

          setImmediate(() => {
            executeEmailsArray();
          });
        } else {
          console.log('Nothing to do. DONE');
          db.close();
        }
      })
      .catch(function (err) {
        console.log('DB ERROR: ' + err.message);
        db.close();
        process.exit();
      });
  })
  .catch((err) => {
    console.log('Can not connect to DB: ' + err.message);
    process.exit();
  });
