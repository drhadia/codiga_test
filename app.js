const express = require("express");
const app = express();
const axios = require("axios");
const cors = require("cors");
const fs = require('fs')
const { mysql_accounting, mysql_pnl, mysql_matchodds, mysql_backlay, mysql_special, mysql_yesno } = require("./mysql_connections");
const uuid = require("uuid");
const model_winLoss = require("./model/win_loss");
const model_bet = require("./model/bet");
const model_stack_data = require("./model/stack_data");
const model_yesno = require("./model/yesnopnl");
const model_moved_bets = require("./model/moved_bets");
const default_stack_data = require("./default_data/stack_data");
const config = require("./config");
const customLib = require("./customLib");
const declareResult = require("./declareResult");
const moment = require('moment')
const requestIp = require('request-ip');
const os = require('os')
const cluster = require('cluster')
const totalCPUs = os.cpus().length

const options = {
    //key: fs.readFileSync(__dirname + '/certificates/ludobetverifykey.pem'),
    //cert: fs.readFileSync(__dirname + '/certificates/ludobetverifycert.pem')
    key: fs.readFileSync(__dirname + '/cert/ludobetverify.key'),
    cert: fs.readFileSync(__dirname + '/cert/server.crt'),
    ca: fs.readFileSync(__dirname + '/cert/INTERMEDIATE.cer')
};
const https = require("https").createServer(options, app);

const io = require('socket.io')(https, {
    cors: {
        origin: '*',
    }
})

cacheTblAdminData = {}
cacheStackData = {}
cacheMarketTypeFromMatchId = {}
matchOddCache = {}
matchBacklayCache = {}
matchYesnoCache = {}
matchSpecialCache = {}
let Watchers = []
let cwincloss = {}
let cwinclossYesNo = {}
let cwinclossSpecial = {}
let cwinclossBacklay = {}
port = process.env.PORT || 3000
https.listen(port, () => console.log("-------------------------------\n|    Bet server is running on port : " + port + "   |\n-------------------------------"));

initSocket();

const redisClient = customLib.redisClient;
const mongoose = customLib.mongoose;

const config_url = {
    apiKey: process.env.API_KEY_GetMatchMarketID,
    seriesId: process.env.SERIES_ID,
    type: "RAW",
};
Object.freeze(config.apiUrls, config_url);

app.use(cors());
app.use(express.json()); // to support JSON-encoded bodies
app.use(
    express.urlencoded({
        // to support URL-encoded bodies
        extended: true,
    })
);
app.set('trust proxy', true)
app.use(requestIp.mw())

app.get(`/`, function (req, res) {
    res.send({ status: false, response: ["Welcome"] });
});

app.get(`/loaderio-711e0fbc70cf8cf1ae1cc440f6c21173`, async function (req, res) {
    const file = './loaderio-711e0fbc70cf8cf1ae1cc440f6c21173/loaderio-711e0fbc70cf8cf1ae1cc440f6c21173.txt';
    fs.readFile(file, 'utf8', (err, data) => {
        if (err) {
            console.error(err);
            return "Error in reading file";
        }
        res.send(data)
    });
})

app.get(`/get-my-ip`, async function (req, res) {
    ipAddress = req.clientIp
    res.send({ status: true, response: { 'ip_address': ipAddress } });
})

app.get(`/generate-cache`, async function (req, res) {
    cacheTblAdminDataGenerate()
    res.send({ status: true, response: { 'cache': 'Generated' } });
})

app.get(`/read-cache`, async function (req, res) {
    res.send({
        status: true,
        response: {
            matchOddCache: matchOddCache,
            matchBacklayCache: matchBacklayCache,
            matchYesnoCache: matchYesnoCache,
            matchSpecialCache: matchSpecialCache,
        }
    });
});

app.post(`/verify`, async function (req, res) {
    clientIp = req.clientIp;
    const { matchId, markID, OperatorID, UserID, Currency, checksum } = customLib.cleanTheBody(req.body);
    console.log('----VERIFY START----');
    console.log("body", req.body)
    if (!await getTblAdminData(OperatorID)) {
        res.send({
            status: false,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 2,
            errorDescription: config.errCode[2],
            checksum: checksum
        });
        return;
    }
    isVerifiedId = ip_verification(clientIp, OperatorID, req.headers.apiKey);
    if (!isVerifiedId) {
        res.send({
            status: false,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 4,
            errorDescription: config.errCode[4],
            checksum: checksum
        });
        return;
    }
    let isValidApiKey = await apiKeyVerification(OperatorID, req.headers.apikey);
    if (!isValidApiKey) {
        res.send({
            status: false,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 12,
            errorDescription: config.errCode[12],
            checksum: checksum
        });
        return;
    }

    const cacheData = await getTblAdminData(OperatorID)
    if (cacheData == '' || cacheData.currency != Currency) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 10,
            errorDescription: config.errCode[10],
            checksum: checksum
        });
        return;
    }

    getMarketTypeFromMatchId(markID, matchId).then(async function (matchTypeData) {
        if (matchTypeData.API_Type == "MatchOdds") {
            verifyMatchOdd(req, res);
        } else if (matchTypeData.API_Type == "backlay") {
            verifyBackLayFancy(req, res)
        } else if (matchTypeData.API_Type == "special") {
            verifySpecialFancy(req, res);
        } else if (matchTypeData.API_Type == "yesno") {
            verifyYesNo(req, res);
        } else {
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: 6,
                errorDescription: config.errCode[6],
                checksum: checksum
            });
            return;
        }
    });
})

app.post(`/selectionid`, async function (req, res) {
    generateCwinClossEntries();
    res.send({
        status: true,
        message: "Initial entry has been created"
    });
    return;
});

app.post(`/selectionid-update`, async function (req, res) {
    customLib.connectMongoDB();
    response = []
    //response = await generatePNL(true);
    res.send({
        status: true,
        message: "Success",
        data: response
    });
    return;
});

app.post(`/declare-result`, async function (req, res) {
    //await calculateResult();
    res.send({
        status: true,
        message: "Success",
    });
    return;
})

app.post(`/manipulation`, async function (req, res) {
    customLib.connectMongoDB();
    //await generatePNL();
    //response = await getManipulation(req);
    response = []
    res.send({
        status: true,
        message: "Success",
        data: response
    });
    return;
});

app.post(`/get-bet-info`, async function (req, res) {
    const { guid } = customLib.cleanTheBody(req.body);
    mysql_accounting.getConnection(function (err, con) {
        if (err) {
            realeaseSQLCon(con);
            return false;
        }
        con.query(
            "SELECT * FROM `bet` WHERE `guid`=?",
            [guid],
            async function (err, result, fields) {
                if (err) {
                    console.log(err);
                    realeaseSQLCon(con);
                } else {
                    result.forEach(res => {
                        console.log(res.operatorid);
                        return;
                    })
                }
            }
        );
    });
    res.send({
        status: true,
        message: "Success"
    });
    return;
});

app.post(`/get-live-bets`, async function (req, res) {
    customLib.connectMongoDB();

    const { is_matched, operator_id, match_id, sport_id, start_date, start_time, end_date, end_time } = customLib.cleanTheBody(req.body);

    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    filter = { status: 1 }

    if (typeof is_matched != 'undefined') {
        if (is_matched != 1) {
            filter['status'] = { $nin: [1] }
        }
    }

    if (typeof operator_id != 'undefined') {
        filter['operatorid'] = operator_id
    }

    if (typeof market_id != 'undefined') {
        filter['marketid'] = market_id
    }

    if (typeof match_id != 'undefined') {
        filter['matchid'] = match_id
    }

    if (typeof sport_id != 'undefined') {
        filter['sportsid'] = sport_id
    }

    if (typeof start_date != 'undefined' || typeof end_date != 'undefined') {
        filter['datetimestamp'] = {};
    }

    if (typeof start_date != 'undefined') {
        start_time_filter = [0, 0];
        //start_filter = start_date;
        if (typeof start_time != 'undefined') {
            start_time_filter = start_time.split(":");
            //start_filter = start_date + " " + start_time
        }
        filter['datetimestamp']['$gte'] = new Date(new Date(start_date).setHours(start_time_filter[0], start_time_filter[1]));
        //filter['datetimestamp']['$gte'] = new Date(start_filter)
    }

    if (typeof end_date != 'undefined') {
        end_time_filter = [23, 59];
        if (typeof end_time != 'undefined') {
            end_time_filter = end_time.split(":");
        }
        filter['datetimestamp']['$lte'] = new Date(new Date(end_date).setHours(end_time_filter[0], end_time_filter[1]));
    }

    betData = await betModel.find(filter);

    res.send({
        status: true,
        message: "Success",
        data: {
            bet_data: betData,
            status_code: configData.errCode
        }
    });
    return;
});

app.post(`/get-bet-verification-report`, async function (req, res) {
    customLib.connectMongoDB();

    const {
        is_matched, operator_id, user_id, match_id, market_id,
        sport_id, market_type, start_date, start_time, end_date, end_time, ip_address, back_lay } = customLib.cleanTheBody(req.body);


    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    filter = { status: 1 }

    if (typeof is_matched != 'undefined') {
        if (is_matched != 1) {
            filter['status'] = { $nin: [1] }
        }
    }

    if (typeof operator_id != 'undefined') {
        filter['operatorid'] = operator_id
    }

    if (typeof user_id != 'undefined') {
        filter['userid'] = user_id
    }

    if (typeof ip_address != 'undefined') {
        filter['ip_address'] = ip_address
    }

    if (typeof market_id != 'undefined') {
        filter['marketid'] = market_id
    }

    if (typeof match_id != 'undefined') {
        filter['matchid'] = match_id
    }

    if (typeof sport_id != 'undefined') {
        filter['sportsid'] = sport_id
    }

    if (typeof market_type != 'undefined') {
        filter['market_type'] = market_type
    }

    if (typeof back_lay != 'undefined') {
        filter['backlayyesno'] = back_lay
    }

    if (typeof start_date != 'undefined' || typeof end_date != 'undefined') {
        filter['datetimestamp'] = {};
    }

    if (typeof start_date != 'undefined') {
        start_time_filter = [0, 0];
        //start_filter = start_date;
        if (typeof start_time != 'undefined') {
            start_time_filter = start_time.split(":");
            //start_filter = start_date + " " + start_time
        }
        filter['datetimestamp']['$gte'] = new Date(new Date(start_date).setHours(start_time_filter[0], start_time_filter[1]));
        //filter['datetimestamp']['$gte'] = new Date(start_filter)
    }

    if (typeof end_date != 'undefined') {
        end_time_filter = [23, 59];
        if (typeof end_time != 'undefined') {
            end_time_filter = end_time.split(":");
        }
        filter['datetimestamp']['$lte'] = new Date(new Date(end_date).setHours(end_time_filter[0], end_time_filter[1]));
    }


    betData = await betModel.find(filter).sort({datetimestamp: -1});
    guidTemp = '';
    let finalBetData = {}

    for (const element of betData) {
        let guid = element.guid;
        guidTemp += "'" + guid + "',"
        finalBetData[guid] = {
            is_copied: ((typeof element.is_copied != 'undefined') ? element.is_copied : 0),
            guid: element.guid,
            operatorid: element.operatorid,
            sportsid: element.sportsid,
            matchid: element.matchid,
            marketid: element.marketid,
            userid: element.userid,
            fancy_name: element.fancy_name,
            backlayyesno: element.backlayyesno,
            beton: element.beton,
            odds: element.odds,
            wins_status: element.wins_status,
            stake: element.stake,
            currency: element.currency,
            exposure_pnl: element.exposure_pnl,
            status: element.status,
            ip_address: element.ip_address,
            datetimestamp: element.datetimestamp,
            is_verified: ((typeof element.is_verified != 'undefined') ? element.is_verified : 0)
        }
    }

    /* guidTemp = guidTemp.replace(/.$/, '')

    query = "SELECT * FROM bet WHERE guid IN(" + guidTemp + ")"
    result = await new Promise((resolve, reject) => {
        mysql_accounting.query(
            query,
            (err, result) => {
                if (err) {
                    console.log("Error in mysql query");
                    console.log(err);
                    return reject(false);
                }
                return resolve(result);
            });
    });
    if (result && result.length > 0) {
        result.forEach(res => {
            if (typeof finalBetData[res.guid] != 'undefined') {
                finalBetData[res.guid]['is_copied'] = 1
            }
        })
    } */
    res.send({
        status: true,
        message: "Success",
        data: finalBetData
    });
    return;
});

app.post(`/delete-verified-bets`, async function (req, res) {
    const { guidList } = customLib.cleanTheBody(req.body);
    if (guidList.length <= 0) {
        res.send({
            status: false,
            message: "Please select bets"
        });
        return
    }
    customLib.connectMongoDB();

    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    guidStr = '';
    deletedBets = [];
    notDeletedBets = [];
    is_verifiedBets = [];
    is_notVerifiedBets = [];
    if (guidList.length > 0) {
        let betData = await betModel.find({ guid: { $in: guidList }, is_verified: 1 });
        console.log("sachin", betData);

        if (betData.length > 0) {
            for (const element of betData) {
                is_verifiedBets.push(element.guid);
                let backupBetsData = {
                    operatorid: element.operatorid,
                    sportsid: element.sportsid,
                    matchid: element.matchid,
                    marketid: element.marketid,
                    userid: element.userid,
                    market_type: element.market_type,
                    mfancyid: element.mfancyid,
                    fancy_family: element.fancy_family,
                    fancy_name: element.fancy_name,
                    guid: element.guid,
                    datetimestamp: element.datetimestamp
                }
                if (await createBetBackup(backupBetsData)) {
                    deleteData = await betModel.deleteOne({ guid: element.guid }).then(function () {
                        deletedBets.push(element.guid);
                    }).catch(function (error) {
                        notDeletedBets.push(element.guid);
                    });
                }
            }
        } 

        const filteredArray = guidList.filter(value => !is_verifiedBets.includes(value));        
    }

    res.send({
        status: true,
        message: "Success"
    });
    return;
});

app.post(`/mark-verify-bets`, async function (req, res) {
    const { guidList } = customLib.cleanTheBody(req.body);
    if (guidList.length <= 0) {
        res.send({
            status: false,
            message: "Please select bets"
        });
        return
    }
    customLib.connectMongoDB();

    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    is_copiedBets = [];
    if (guidList.length > 0) {
        let betData = await betModel.find({ guid: { $in: guidList }, is_copied: 1 });
        for (const element of betData) {
            is_copiedBets.push(element.guid);
        }

        await betModel.updateMany({ guid: { $in: is_copiedBets } }, { is_verified: 1 }, function (err, result) {
        }).clone();
    }

    res.send({
        status: true,
        message: "Success"
    });
    return;
});

app.post(`/copy-to-mysql`, async function (req, res) {
    const { guidList } = customLib.cleanTheBody(req.body);
    if (guidList.length <= 0) {
        res.send({
            status: false,
            message: "Please select bets"
        });
        return
    }
    customLib.connectMongoDB();

    //var guidStr = "'" + guidList.join("','") + "'";

    let betModel = null
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    let bets = betModel.find({ guid: { $in: guidList } })
    copiedBets = []
    notCopiedBets = []
    alreadyExistBetsInMysql = []
    if (bets.length > 0) {
        for (const bet of bets) {
            guid = bet.guid
            if (bet.status == 1 && (bet.wins_status == '' || bet.wins_status == null)) {
                //declare result and copy to mysql
                let result = {}
                const { matchid, operatorid, market_type } = customLib.cleanTheBody(bet)
                if (market_type == config.markettype.MATCHODDS) {
                    result = await declareResult.calculateOddResult(bet)
                } else if (market_type == config.markettype.BACKLAY) {
                    result = await declareResult.calculateBackLayfancyResult(bet)
                } else if (market_type == config.markettype.SPECIAL) {
                    result = await declareResult.calculateSpecialfancyResult(bet)
                } else if (market_type == config.markettype.YESNO) {
                    result = await declareResult.calculateYesnofancyResult(bet)
                }

                if (Object.keys(result).length > 0) {
                    try {
                        if (result["wins_status"] != '') {
                            if (result["wins_status"] == "hold") {
                                await Bet.updateOne({ guid: bet.guid }, { wins_status: result["wins_status"] }, function (err, result) {
                                }).clone();
                            }
                            let data = await getTblAdminData(operatorid)
                            const { multiple_factor } = data
                            final_profit_loss = result["profit_loss"] * multiple_factor * -1
                            win_selection_id = (typeof result["win_selection_id"] != 'undefined') ? result["win_selection_id"] : ''
                            await Bet.updateOne({ guid: bet.guid }, {
                                wins_status: result["wins_status"],
                                profit_loss: result["profit_loss"], //this is for customer profit loss
                                final_profit_loss: final_profit_loss //this is for company profit loss
                            }, function (err, result) {
                                if (err) {
                                    console.log('Error while updating result in mongodb');
                                    console.log(err);
                                }
                            }).clone();
                            if (win_selection_id != '' && win_selection_id != 0) {
                                await insertMatchWinner(matchid, win_selection_id);
                            }
                            let saved = await saveToMySql(bet)
                            if (saved == false) {
                                notCopiedBets.push(guid)
                            } else {
                                copiedBets.push(guid)
                            }
                        }
                    } catch (e) {
                        console.log('Exception while calculateResult in mongodb');
                        console.log(e);
                    }
                }
            } else {
                await new Promise((resolve, reject) => {
                    mysql_accounting.getConnection(function (err, con) {
                        if (err) {
                            notCopiedBets.push(guid)
                            con.release()
                            return reject(false);
                        }
                        con.query(
                            "SELECT * FROM bet WHERE guid = ?",
                            [guid],
                            async function (err, result, fields) {
                                con.release()
                                if (err) {
                                    notCopiedBets.push(guid)
                                    console.log("Error in mysql query to update user pnl");
                                    console.log(err);
                                    return reject(false);
                                }
                                if (result.length > 0) { //already exists
                                    alreadyExistBetsInMysql.push(guid)
                                } else { //not exists in mysql, so copy it
                                    let saved = await saveToMySql(bet)
                                    if (saved == false) {
                                        notCopiedBets.push(guid)
                                    } else {
                                        copiedBets.push(guid)
                                    }
                                }
                                return resolve(result);
                            }
                        );
                    });
                });
            }
        }
    }
    res.send({
        status: true,
        message: "Success"
    });
})

async function saveToMySql(bet) {
    return await new Promise((resolve, reject) => {
        mysql_accounting.getConnection(function (err, con) {
            if (err) {
                con.release()
                return reject(false);
            }
            con.query(
                "INSERT IGNORE INTO `bet`(`operatorid`, `matchid`, `sportsid`, `userid`, `mfancyid`, `market_type`, `fancy_family`, `fancy_name`, `fancy_reference`, `selectionid`, `marketid`, `beton`, `odds`, `backlayyesno`, `stake`, `profit_loss`, `exposure_pnl`, `status`, `wins_status`, `datetimestamp`, `currency`, `final_stack`, `final_profit_loss`, `final_exposure_pnl`, `guid`, `ip_address`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    bet.operatorid, bet.matchid, bet.sportsid, bet.userid, bet.mfancyid, bet.market_type,
                    bet.fancy_family, bet.fancy_name, bet.fancy_reference, bet.selectionid, bet.marketid,
                    bet.beton, bet.odds, bet.backlayyesno, bet.stake, bet.profit_loss, bet.exposure_pnl,
                    bet.status, bet.wins_status, bet.datetimestamp, bet.currency, bet.final_stack,
                    bet.final_profit_loss, bet.final_exposure_pnl, bet.guid, bet.ip_address
                ],
                async function (err, result, fields) {
                    con.release()
                    if (err) {
                        console.log("Error in mysql query to update user pnl");
                        console.log(err);
                        return reject(false);
                    }
                    return resolve(result);
                }
            );
        });
    });
}

app.post(`/get-exposure`, async function (req, res) {
    customLib.connectMongoDB();

    let { type, operator_id } = customLib.cleanTheBody(req.body);

    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    if (typeof operator_id != 'undefined' && operator_id != null) {
        betModel.aggregate(
            [
                { $match: { operatorid: operator_id } },
                {
                    $group: {
                        _id: null,
                        total_exposure_pnl: { $sum: "$exposure_pnl" }
                    }
                }
            ],
            function (err, docs) {
                if (err) {
                    console.log("ERR : /get-exposure - operator_id")
                    console.log(err);
                    res.send({
                        status: false,
                        message: "Fail",
                        data: []
                    });
                    return;
                }
                res.send({
                    status: true,
                    message: "Success",
                    data: docs
                });
                return;
            }
        );
    } else {
        if (typeof type == 'undefined' || type == null) {
            type = 'all';
        }
        if (type == 'all') {
            betModel.aggregate(
                [
                    {
                        $group: {
                            _id: null,
                            total_exposure_pnl: { $sum: "$exposure_pnl" }
                        }
                    }
                ],
                function (err, docs) {
                    if (err) {
                        console.log("ERR : /get-exposure - type = all")
                        res.send({
                            status: false,
                            message: "Fail",
                            data: []
                        });
                        return;
                    }
                    res.send({
                        status: true,
                        message: "Success",
                        data: docs
                    });
                    return;
                }
            );
        } else {
            betModel.aggregate(
                [
                    {
                        $group:
                        {
                            _id: { operatorid: "$operatorid" },
                            total_exposer: { $sum: "$exposure_pnl" }
                        }
                    }
                ],
                function (err, docs) {
                    if (err) {
                        console.log("ERR : /get-exposure - type = operator")
                        res.send({
                            status: false,
                            message: "Fail",
                            data: []
                        });
                        return;
                    }
                    res.send({
                        status: true,
                        message: "Success",
                        data: docs
                    });
                    return;
                }
            );
        }
    }
    return;
});

app.post('/get-data', async function (req, res) {
    let { type } = customLib.cleanTheBody(req.body);
    if (typeof type == 'undefined') {
        type = 'all';
    }

    customLib.connectMongoDB();
    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    responseData = {}
    matches = markets = sports = {};
    if (type == 'match') {
        await betModel.distinct('matchid')
            .then(docs => {
                matches = docs;
            });
        responseData['matches'] = matches
    } else if (type == 'market') {
        await betModel.distinct('marketid')
            .then(docs => {
                markets = docs;
            });
        responseData['markets'] = markets
    } else if (type == 'sport') {
        await betModel.distinct('sportsid')
            .then(docs => {
                sports = docs;
            });
        responseData['sports'] = sports
    } else {
        await betModel.distinct('matchid')
            .then(docs => {
                matches = docs;
            });
        await betModel.distinct('marketid')
            .then(docs => {
                markets = docs;
            });
        await betModel.distinct('sportsid')
            .then(docs => {
                sports = docs;
            });
        responseData['matches'] = matches
        responseData['markets'] = markets
        responseData['sports'] = sports
    }
    res.send({
        status: true,
        message: "Success",
        data: responseData
    });
    return;
});

app.get('/get-exposure-details', async function (req, res) {
    customLib.connectMongoDB();
    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    let marketData = [];
    betModel.aggregate(
        [
            {
                $group:
                {
                    _id: { market: "$marketid" },
                    //total_exposer: { $sum: "$exposure_pnl" }
                }
            }
        ],
        function (err, docs) {
            if (err) {
                console.log("ERR : /get-exposure - type = operator")
                res.send({
                    status: false,
                    message: "Fail",
                    data: []
                });
                return;
            }

            docs.forEach(element => {
                console.log(element);
            })

            res.send({
                status: true,
                message: "Success",
                data: docs
            });
            return;
        }
    );
    return;
});

app.post('/get-result', async function (req, res) {
    const { operator_id, guids } = req.body
    if (typeof operator_id == 'undefined' || operator_id == '') {
        res.send({ status: false, message: 'Please provide operator id' })
        return
    }
    if (typeof guids == 'undefined' || guids == '' || guids.length <= 0) {
        res.send({ status: false, message: 'Please provide guid' })
        return
    }

    let newGuid = guids.join([separator = "','"]) //prepare comma seprated string from array
    newGuid = "'" + newGuid + "'"
    query = `SELECT * FROM bet WHERE operatorid = ${operator_id} AND guid IN (${newGuid})`
    result = await new Promise((resolve, reject) => {
        mysql_accounting.query(
            query,
            (err, result) => {
                if (err) {
                    console.log("Error in mysql query");
                    console.log(err);
                    //pool.release()
                    return reject(false);
                }
                return resolve(result);
            });
    });
    status = false
    data = []
    if (typeof result != 'undefined' && result.length > 0) {
        status = true
        data = result
    }
    res.send({ status: status, data: data })
    return;
});

async function moveToHistory() {
    console.log("starting moveToHistory")
    customLib.connectMongoDB();

    let betModel = null
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    let cwinClossModel = null;
    if (mongoose.models.cwin_closs) {
        cwinClossModel = mongoose.model('cwin_closs');
    } else {
        cwinClossModel = mongoose.model('cwin_closs', model_winLoss);
    }

    let yesNoPnlModel = null;
    if (mongoose.models.yesno) {
        yesNoPnlModel = mongoose.model('yesNOPNL');
    } else {
        yesNoPnlModel = mongoose.model('yesNOPNL', model_yesno);
    }

    let betData = await betModel.find({ wins_status: { $nin: [''] } });
    for (const bet of betData) {
        console.log("start for ", bet.guid)
        const {
            matchid, operatorid, sportsid, userid, mfancyid, selectionid, marketid, beton, odds,
            backlayyesno, exposure_pnl, stake, profit_loss, wins_status, currency, final_stack,
            final_exposure_pnl, final_profit_loss, guid, status, datetimestamp, fancy_family,
            fancy_name, fancy_reference, ip_address, market_type
        } = customLib.cleanTheBody(bet)


        let moveRecord = await new Promise((resolve, reject) => {
            mysql_accounting.getConnection(function (err, con) {
                if (err) {
                    realeaseSQLCon(con);
                    return reject(false);
                }
                console.log("market_type", market_type)
                con.query(
                    "INSERT IGNORE INTO `bet`(`operatorid`, `matchid`, `sportsid`, `userid`, `mfancyid`, `market_type`, `fancy_family`, `fancy_name`, `fancy_reference`, `selectionid`, `marketid`, `beton`, `odds`, `backlayyesno`, `stake`, `profit_loss`, `exposure_pnl`, `status`, `wins_status`, `datetimestamp`, `currency`, `final_stack`, `final_profit_loss`, `final_exposure_pnl`, `guid`, `ip_address`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        operatorid, matchid, sportsid, userid, mfancyid, market_type, fancy_family, fancy_name, fancy_reference,
                        selectionid, marketid, beton, odds, backlayyesno, stake, profit_loss,
                        exposure_pnl, status, wins_status, datetimestamp, currency, final_stack,
                        final_profit_loss, final_exposure_pnl, guid, ip_address
                    ],
                    async function (err, result, fields) {
                        realeaseSQLCon(con);
                        console.log("inserted in DB : ", guid)
                        customLib.log(guid, "move_to_history")
                        if (err) {
                            console.log("Error in mysql query to update user pnl");
                            console.log(err);
                            return reject(false);
                        }

                        let movedBetsData = {
                            operatorid: operatorid,
                            sportsid: sportsid,
                            matchid: matchid,
                            marketid: marketid,
                            userid: userid,
                            market_type: market_type,
                            mfancyid: mfancyid,
                            fancy_family: fancy_family,
                            fancy_name: fancy_name,
                            guid: guid,
                            datetimestamp: datetimestamp
                        }
                        await createBetBackup(movedBetsData)

                        /* console.log("deleting start for  : ", guid)
                        deleteData = await betModel.deleteOne({ guid: guid }).then(function () {
                            customLib.log(guid, "deleted_from_mongo_matched")
                        }).catch(function (error) {
                            customLib.log(guid, "unable_to_delete_from_mongo_matched")
                        }); */

                        updateData = await betModel.updateOne({ guid: guid }, {is_copied: 1}).then(function () {
                            //customLib.log(guid, "deleted_from_mongo_matched")
                        }).catch(function (error) {
                            //customLib.log(guid, "unable_to_delete_from_mongo_matched")
                        });


                        cwinclossFilter = {
                            markettype: market_type,
                            matchid: matchid,
                            marketid: marketid
                        }
                        if (market_type == config.markettype.MATCHODDS) {
                            //TODO: Add extra condition if required
                        } else if (market_type == config.markettype.YESNO) {
                            cwinclossFilter['mfancyid'] = mfancyid
                            let yesnopnlFilter = cwinclossFilter
                            delete yesnopnlFilter['markettype']
                            yesNoPnlModel.deleteMany(yesnopnlFilter).then(function () {
                                console.log("yesnopnl entry deleted"); // Success
                            }).catch(function (error) {
                                console.log("Err in yesnopnl entry delete", error); // Success
                            });
                        } else if (market_type == config.markettype.BACKLAY) {
                            cwinclossFilter['mfancyid'] = mfancyid
                        } else if (market_type == config.markettype.SPECIAL) {
                            cwinclossFilter['mfancyid'] = mfancyid
                        }
                        console.log("Deleting cwincloss entry for ", cwinclossFilter)
                        await cwinClossModel.deleteMany(cwinclossFilter).then(function () {
                            console.log("cwincloss entry deleted"); // Success
                        }).catch(function (error) {
                            console.log("Err in cwincloss entry delete", error); // Success
                        });
                        console.log("End deleting cwincloss entry")

                        console.log("deleting end for  : ", guid)
                        return resolve(result);
                    }
                );
            });
        });
        console.log("end for ", bet.guid)
    }
    console.log("end moveToHistory")
}

async function getPnl(req) {
    let sportsId = 1000; //manipulation API is only for sportsId = 1000
    const { matchId } = customLib.cleanTheBody(req.body);
    if (mongoose.models.cwin_closs) {
        cwin_closs = mongoose.model('cwin_closs');
    } else {
        cwin_closs = mongoose.model('cwin_closs', model_winLoss);
    }
    cwindata = await cwin_closs.find({ matchid: matchId, sportsid: sportsId });
    return pnlResponseForAllType(cwindata);
}

async function getYesNoPnl(req) {
    let sportsId = 1000; //manipulation API is only for sportsId = 1000
    const { matchId, LinkedRound } = customLib.cleanTheBody(req.body);

    if (mongoose.models.yesno) {
        yesno = mongoose.model('yesNOPNL');
    } else {
        yesno = mongoose.model('yesNOPNL', model_yesno);
    }

    yesnodata = await yesno.find({
        matchid: matchId,
        sportsid: sportsId,
        linkedround: LinkedRound
    });
    return pnlResponseForAllType(yesnodata);
}

async function getSpecialPnl(req) {
    let sportsId = 1000; //manipulation API is only for sportsId = 1000
    const { matchId, LinkedRound } = customLib.cleanTheBody(req.body);

    if (mongoose.models.cwin_closs) {
        cwin_closs = mongoose.model('cwin_closs');
    } else {
        cwin_closs = mongoose.model('cwin_closs', model_winLoss);
    }

    cwindata = await cwin_closs.find({
        matchid: matchId,
        sportsid: sportsId,
        linkedround: LinkedRound
    });
    return pnlResponseForAllType(cwindata);
}

async function getManipulation(req) {
    yesNoPnl = await getYesNoPnl(req);
    specialPnl = await getSpecialPnl(req);
    oddPnl = await getPnl(req);
    return { yesNoPnl, specialPnl, oddPnl };
}

async function pnlResponseForAllType(datas) {
    let response = []
    pnl = '';
    if (datas.length != 0) {
        for (const data of datas) {
            if (data.linkedround) {
                if (data.fancyreference) {
                    pnl += data.selectionidname + "(" + data.calpnl + "), "
                } else {
                    pnl += data.beton + "(" + data.calpnl + "), "
                }
            } else {
                pnl += data.selectionidname + "(" + data.calpnl + "), "
            }
        }
        response.push({
            matchid: datas[0]['matchid'],
            pnl: pnl
        })
    }
    return response;
}

function realeaseSQLCon(con = {}) {
    try {
        con.release();
    } finally {
        return true;
    }
}

async function updateBetMongodbyesno(options = {}) {
    customLib.connectMongoDB();
    var yesno = null;
    if (mongoose.models.yesno) {
        yesno = mongoose.model('yesNOPNL');
    } else {
        yesno = mongoose.model('yesNOPNL', model_yesno);
    }

    const {
        SportID,
        MatchID,
        MfancyID,
        MarketID,
        BetON,
        LinkedRound,
        fancyreference,
        pnl,
        cpnl
    } = options;

    let tmpPnl = pnl
    let tmpCpnl = cpnl
    if (typeof tmpPnl == 'undefined') {
        tmpPnl = 0
    }
    if (typeof tmpCpnl == 'undefined') {
        tmpCpnl = 0
    }

    var insert_yesno = new yesno({
        sportsid: SportID,
        marketid: MarketID,
        matchid: MatchID,
        mfancyid: MfancyID,
        beton: BetON,
        linkedround: LinkedRound,
        fancyreference: fancyreference,
        pnl: tmpPnl,
        cpnl: tmpCpnl,
    });


    return insert_yesno.save().then(function (savedData) {
        return true;
    }).catch(function (err) {
        return false;
    });
}

async function insertBetMongodb(options = {}, fancyData = {}) {
    customLib.connectMongoDB();

    var Bet = null;
    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }

    const {
        OperatorID, MatchID, SportID, UserID, MfancyID,
        SelectionID, MarketID, BetON, BetOdds, BackLayYesNo,
        Stake, Currency, Status, Exposure_pnl, GUID, fancy_family,
        fancy_name, fancy_reference, ip_address, market_type, RoundLinked
    } = options;

    operatorkey = OperatorID;
    data = await getTblAdminData(operatorkey)
    const { multiple_factor } = data
    let final_stack = Stake * multiple_factor;
    let final_exposure = Exposure_pnl * multiple_factor;

    let record = {
        operatorid: OperatorID,
        matchid: MatchID,
        sportsid: SportID,
        userid: UserID,
        mfancyid: MfancyID,
        fancy_family: fancy_family,
        fancy_name: fancy_name,
        fancy_reference: fancy_reference,
        market_type: market_type,
        selectionid: SelectionID,
        marketid: MarketID,
        beton: BetON,
        odds: BetOdds,
        backlayyesno: BackLayYesNo,
        exposure_pnl: Exposure_pnl,
        stake: Stake,
        currency: Currency,
        final_stack: final_stack.toFixed(config.decimalPlace),
        final_exposure_pnl: final_exposure.toFixed(config.decimalPlace),
        guid: GUID,
        status: Status,
        wins_status: "",
        is_pnl_calculated: 0,
        ip_address: ip_address
    }
    if (typeof RoundLinked != 'undefined') {
        record['round_linked'] = RoundLinked
    }

    var betModel = new Bet(record);
    return betModel.save().then(async function (result) {
        if (Status == 1) {
            sendDataForPnl(result);
            console.log("saving to log file")
            customLib.log(result.guid, "save_to_mongo")
        }
        return true;
    }).catch(function (err) {
        console.log("err in insertBetMongodb", err)
        return false;
    });
}

async function calculatePnl(market_type = "", bet = {}, fancyData = {}) {
    console.log("fancyData", fancyData)
    if (market_type == 'MatchOdds') {
        calculateMatchOddsPNL(bet)
    } else if (market_type == 'YesNo') {
        calulateYesnoPNL(bet, fancyData)
    } else if (market_type == 'SpecialFancy') {
        calculateSpecialPNL(bet, fancyData)
    } else if (market_type == 'BacklayFancy') {
        calculateBacklayPNL(bet, fancyData)
    }
}

async function calculateMatchOddsPNL(bet) {
    var cwinClossPnlModel = null;
    if (mongoose.models.cwin_closs) {
        cwinClossPnlModel = mongoose.model('cwin_closs');
    } else {
        cwinClossPnlModel = mongoose.model('cwin_closs', model_winLoss);
    }

    let betData = await cwinClossPnlModel.find({ sportsid: bet.sportsid, markettype: "MatchOdds", matchid: bet.matchid, marketid: bet.marketid });
    let oddCwincloss = {}
    let deleteOldEntries = false
    if (typeof betData == 'undefined' || !betData || betData.length <= 0) {
        let tempData = []
        let element = matchOddCache[bet.matchid];
        let teamArr = [];
        let totalPlayer = 4;
        for (let i = 1; i <= totalPlayer; i++) {
            let key = "Team" + i;
            teamArr.push(element[key])
        }
        for (let team of teamArr) {
            let insertData = {
                sportsid: element.SportID,
                marketid: element.MarketID,
                matchid: element.MatchID,
                selectionid: team.SelectionID,
                selectionidname: team.Color,
                win: 0,
                cwin: 0,
                loss: 0,
                closs: 0,
                markettype: "MatchOdds"
            }
            tempData.push(insertData)
        }
        oddCwincloss = tempData
    } else {
        deleteOldEntries = true
        oddCwincloss = betData
    }

    for (let entityId = 0; entityId < oddCwincloss.length; entityId++) {
        if (bet.backlayyesno == 0) {
            if (oddCwincloss[entityId].selectionid == bet.selectionid) {
                oddCwincloss[entityId]['pnl'] = (parseFloat(oddCwincloss[entityId]['pnl']) + parseFloat(bet['exposure_pnl'])).toFixed(2);
            } else {
                oddCwincloss[entityId]['pnl'] = (parseFloat(oddCwincloss[entityId]['pnl']) - parseFloat(bet['stake'])).toFixed(2);
            }
        } else {
            if (oddCwincloss[entityId].selectionid == bet.selectionid) {
                oddCwincloss[entityId]['pnl'] = (parseFloat(oddCwincloss[entityId]['pnl']) - parseFloat(bet['exposure_pnl'])).toFixed(2);
            } else {
                oddCwincloss[entityId]['pnl'] = (parseFloat(oddCwincloss[entityId]['pnl']) + parseFloat(bet['stake'])).toFixed(2);
            }
        }
        oddCwincloss[entityId]['cpnl'] = (oddCwincloss[entityId]['pnl']) / 10000
    }
    if (deleteOldEntries) {
        await cwinClossPnlModel.deleteMany({ sportsid: bet.sportsid, market_type: "MatchOdds", matchid: bet.matchid, marketid: bet.marketid });
    }
    await cwinClossPnlModel.insertMany(oddCwincloss)
    pnl = oddCwincloss[0].selectionidname + "(" + oddCwincloss[0]['cpnl'] + ")," + oddCwincloss[1].selectionidname + "(" + oddCwincloss[1]['cpnl'] + ")," + oddCwincloss[2].selectionidname + "(" + oddCwincloss[2]['cpnl'] + ")," + oddCwincloss[3].selectionidname + "(" + oddCwincloss[3]['cpnl'] + ")";
    console.log("odd pnl", "matchid-" + bet.matchid + "-pnl-" + pnl)
}

//async function calulateYesnoPNL(bet, fancyData) {
async function calulateYesnoPNL(bet) {
    var yesnoPnlModel = null;
    if (mongoose.models.yesno) {
        yesnoPnlModel = mongoose.model('yesNOPNL');
    } else {
        yesnoPnlModel = mongoose.model('yesNOPNL', model_yesno);
    }

    //console.log("bet", bet)
    let filters = {
        sportsid: bet.sportsid,
        markettype: "YesNo",
        matchid: bet.matchid,
        marketid: bet.marketid,
        mfancyid: bet.mfancyid
    }
    let cwinclossYesNo = await yesnoPnlModel.find(filters);
    let key = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid + "_" + bet.mfancyid
    console.log("key", key)
    let deleteOldEntries = false
    yesNoPnl = {}
    if (typeof cwinclossYesNo != 'undefined' && cwinclossYesNo.length > 0) {
        deleteOldEntries = true
        for (const yesno of cwinclossYesNo) {
            yesNoPnl[yesno.beton] = yesno.pnl
        }
    }
    console.log("yesNoPnl from table", yesNoPnl)
    const beton = parseInt(bet.beton)
    const min = beton - 30;
    const max = beton + 30;

    let arrLen = Object.keys(yesNoPnl).length
    if (arrLen <= 0) {
        for (let i = min; i <= max; i++) {
            yesNoPnl[i] = 0
        }
    } else {
        for (let i = min; i <= max; i++) {
            if (typeof yesNoPnl[i] == 'undefined' || yesNoPnl[i] == null) {
                let len = Object.keys(yesNoPnl).length
                let firstKey = Object.keys(yesNoPnl)[0];
                let firstValue = yesNoPnl[Object.keys(yesNoPnl)[0]];
                let lastKey = Object.keys(yesNoPnl)[len - 1];
                let lastValue = yesNoPnl[Object.keys(yesNoPnl)[len - 1]];
                if (i > lastKey) {
                    yesNoPnl[i] = lastValue
                } else if (i < firstKey) {
                    yesNoPnl[i] = firstValue
                } else {
                    for (let j = 0; j <= len; j++) {
                        let tmpVal = Object.keys(yesNoPnl)[j]
                        if (i < tmpVal) {
                            yesNoPnl[i] = yesNoPnl[tmpVal]
                            break
                        }
                    }
                }
            }
        }
    }

    yesNoPnl = Object.keys(yesNoPnl).sort().reduce(
        (obj, k) => {
            obj[k] = yesNoPnl[k];
            return obj;
        },
        {}
    );

    let len = Object.keys(yesNoPnl).length
    let firstKey = Object.keys(yesNoPnl)[0];
    let lastLast = Object.keys(yesNoPnl)[len - 1];
    for (let i = firstKey; i <= lastLast; i++) {
        if (typeof yesNoPnl[i] == 'undefined' || yesNoPnl[i] == null) {
            let len1 = Object.keys(yesNoPnl).length
            for (let j = 0; j <= len1; j++) {
                let tmpVal = Object.keys(yesNoPnl)[j]
                if (i < tmpVal) {
                    yesNoPnl[i] = yesNoPnl[tmpVal]
                    break
                }
            }
        }
    }

    for (const k in yesNoPnl) {
        if (bet.backlayyesno == 1) {
            if (k < bet.beton) {
                yesNoPnl[k] = (parseFloat(yesNoPnl[k]) + parseFloat(bet.beton)).toFixed(2)
            } else {
                yesNoPnl[k] = (parseFloat(yesNoPnl[k]) - parseFloat(bet.exposure_pnl)).toFixed(2)
            }
        } else {
            if (k < bet.beton) {
                yesNoPnl[k] = (parseFloat(yesNoPnl[k]) - parseFloat(bet.beton)).toFixed(2)
            } else {
                yesNoPnl[k] = (parseFloat(yesNoPnl[k]) + parseFloat(bet.exposure_pnl)).toFixed(2)
            }
        }
    }

    if (deleteOldEntries) {
        await yesnoPnlModel.deleteMany(filters);
    }
    insertYesnoData = []
    for (const k in yesNoPnl) {
        let insertData = {
            sportsid: bet.sportsid,
            matchid: bet.matchid,
            marketid: bet.marketid,
            mfancyid: bet.mfancyid,
            beton: k,
            pnl: yesNoPnl[k],
            cpnl: (yesNoPnl[k]) / 10000,
            markettype: "YesNo",
        }
        insertYesnoData.push(insertData)
    }
    await yesnoPnlModel.insertMany(insertYesnoData)


    pnl = ""
    for (const k in yesNoPnl) {
        pnl += k + "(" + (parseFloat(yesNoPnl[k] / 1000).toFixed(1)) + "),"
    }
    let reference = fancyData.fancyreference
    let RoundLinked = fancyData.linkedround
    console.log("yesNoPnl pnl", "matchid-" + bet.matchid + "-reference-" + reference + "-RoundLinked-" + RoundLinked + "-pnl-" + pnl)
}

async function calculateSpecialPNL(bet, fancyData) {
    var cwinClossPnlModel = null;
    if (mongoose.models.cwin_closs) {
        cwinClossPnlModel = mongoose.model('cwin_closs');
    } else {
        cwinClossPnlModel = mongoose.model('cwin_closs', model_winLoss);
    }
    let filters = {
        sportsid: bet.sportsid,
        markettype: "SpecialFancy",
        matchid: bet.matchid,
        marketid: bet.marketid,
        mfancyid: bet.mfancyid
    }
    let betData = await cwinClossPnlModel.find(filters);
    let key = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid + "_" + bet.mfancyid
    let cwinclossSpecial = []
    let deleteOldEntries = false
    if (typeof betData == 'undefined' || !betData || betData.length <= 0) {
        deleteOldEntries = true
        if (typeof matchSpecialCache[bet.matchid] != 'undefined') {
            if (matchSpecialCache[bet.matchid].length > 0) {
                for (let specialFancy of matchSpecialCache[bet.matchid]) {
                    if (bet.sportsid == specialFancy.SportsID && bet.matchid == specialFancy.MatchID && bet.marketid == specialFancy.MarketID && bet.mfancyid == specialFancy.MFancyID) {
                        for (let i = 1; i <= 6; i++) {
                            let selectionKey = "Odds" + i + "SelectionID"
                            if (typeof specialFancy[selectionKey] != "undefined") {
                                let insertData = {
                                    index: i,
                                    sportsid: specialFancy.SportsID,
                                    marketid: specialFancy.MarketID,
                                    matchid: specialFancy.MatchID,
                                    mfancyid: specialFancy.MFancyID,
                                    selectionid: specialFancy[selectionKey],
                                    win: 0,
                                    cwin: 0,
                                    loss: 0,
                                    closs: 0,
                                    pnl: 0,
                                    cpnl: 0,
                                    markettype: "SpecialFancy"
                                }
                                cwinclossSpecial.push(insertData)
                            }
                        }
                    }
                }
            }
        }
    } else {
        cwinclossSpecial = betData
    }

    if (cwinclossSpecial.length > 0) {
        for (let entityId = 0; entityId < cwinclossSpecial.length; entityId++) {
            if (bet.backlayyesno == 0) {
                if (cwinclossSpecial[entityId].selectionid == bet.selectionid) {
                    cwinclossSpecial[entityId]['pnl'] = (parseFloat(cwinclossSpecial[entityId]['pnl']) + parseFloat(bet['exposure_pnl'])).toFixed(2);
                } else {
                    cwinclossSpecial[entityId]['pnl'] = (parseFloat(cwinclossSpecial[entityId]['pnl']) - parseFloat(bet['stake'])).toFixed(2);
                }
            } else {
                if (cwincloss[entityId].selectionid == bet.selectionid) {
                    cwinclossSpecial[entityId]['pnl'] = (parseFloat(cwinclossSpecial[entityId]['pnl']) - parseFloat(bet['exposure_pnl'])).toFixed(2);
                } else {
                    cwinclossSpecial[entityId]['pnl'] = (parseFloat(cwinclossSpecial[entityId]['pnl']) + parseFloat(bet['stake'])).toFixed(2);
                }
            }
            cwinclossSpecial[entityId]['cpnl'] = (cwinclossSpecial[entityId]['pnl']) / 10000
        }
    }
    if (cwinclossSpecial.length > 0) {
        if (deleteOldEntries) {
            await cwinClossPnlModel.deleteMany(filters);
        }
        await cwinClossPnlModel.insertMany(cwinclossSpecial)
    }
    pnl = ""
    for (let entityId = 0; entityId < cwinclossSpecial.length; entityId++) {
        pnl += cwinclossSpecial[entityId].index + "(" + cwinclossSpecial[entityId]['cpnl'] + "),"
    }

    reference = fancyData.FancyReference
    console.log("cwinclossSpecial pnl", "matchid-" + bet.matchid + "-reference-" + reference + "-pnl-" + pnl)
}

async function calculateBacklayPNL(bet, fancyData) {
    var cwinClossPnlModel = null;
    if (mongoose.models.cwin_closs) {
        cwinClossPnlModel = mongoose.model('cwin_closs');
    } else {
        cwinClossPnlModel = mongoose.model('cwin_closs', model_winLoss);
    }
    let filters = {
        sportsid: bet.sportsid,
        markettype: "BacklayFancy",
        matchid: bet.matchid,
        marketid: bet.marketid,
    }
    let betData = await cwinClossPnlModel.find(filters);
    let key = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid
    let cwinclossBacklay = []
    let deleteOldEntries = false
    if (typeof betData == 'undefined' || !betData || betData.length <= 0) {
        deleteOldEntries = true
        if (typeof cwinclossBacklay[key] == 'undefined') {
            if (typeof matchBacklayCache[bet.matchid] != 'undefined') {
                if (matchBacklayCache[bet.matchid].length > 0) {
                    for (let backlayFancy of matchBacklayCache[bet.matchid]) {
                        if (bet.sportsid == backlayFancy.SportsID && bet.matchid == backlayFancy.MatchID && bet.marketid == backlayFancy.MarketID) {
                            let insertData = {
                                sportsid: backlayFancy.SportsID,
                                marketid: backlayFancy.MarketID,
                                matchid: backlayFancy.MatchID,
                                mfancyid: backlayFancy.MFancyID,
                                win: 0,
                                cwin: 0,
                                loss: 0,
                                closs: 0,
                                pnl: 0,
                                cpnl: 0,
                                fancy_reference: backlayFancy.FancyReference,
                                markettype: "BacklayFancy",
                            }
                            cwinclossBacklay.push(insertData)

                        }
                    }
                }
            }
        }
    } else {
        cwinclossBacklay = betData
    }

    if (typeof cwinclossBacklay != 'undefined') {
        let cwincloss = cwinclossBacklay
        for (let entityId = 0; entityId < cwincloss.length; entityId++) {
            if (bet.backlayyesno == 0) {
                if (cwincloss[entityId].mfancyid == bet.mfancyid) {
                    cwinclossBacklay[entityId]['pnl'] = (parseFloat(cwinclossBacklay[entityId]['pnl']) + parseFloat(bet['exposure_pnl'])).toFixed(2);
                } else {
                    cwinclossBacklay[entityId]['pnl'] = (parseFloat(cwinclossBacklay[entityId]['pnl']) - parseFloat(bet['stake'])).toFixed(2);
                }
            } else {
                if (cwincloss[entityId].mfancyid == bet.mfancyid) {
                    cwinclossBacklay[entityId]['pnl'] = (parseFloat(cwinclossBacklay[entityId]['pnl']) - parseFloat(bet['exposure_pnl'])).toFixed(2);
                } else {
                    cwinclossBacklay[entityId]['pnl'] = (parseFloat(cwinclossBacklay[entityId]['pnl']) + parseFloat(bet['stake'])).toFixed(2);
                }
            }
            cwinclossBacklay[entityId]['cpnl'] = (cwinclossBacklay[entityId]['pnl']) / 10000
        }
    }
    if (cwinclossBacklay.length > 0) {
        if (deleteOldEntries) {
            await cwinClossPnlModel.deleteMany(filters);
        }
        await cwinClossPnlModel.insertMany(cwinclossBacklay)
    }
    let killNo = fancyData.KillNo
    pnl = cwinclossBacklay[0].fancy_reference + "(" + cwinclossBacklay[0]['cpnl'] + ")," + cwinclossBacklay[1].fancy_reference + "(" + cwinclossBacklay[1]['cpnl'] + ")," + cwinclossBacklay[2].fancy_reference + "(" + cwinclossBacklay[2]['cpnl'] + ")," + cwinclossBacklay[3].fancy_reference + "(" + cwinclossBacklay[3]['cpnl'] + ")";
    console.log("backlay pnl", "matchid-" + bet.matchid + "-killNo-" + killNo + "-pnl-" + pnl)
}

function sendDataForPnl(data) {
    const {
        matchid, sportsid, userid, mfancyid, selectionid, marketid, beton, odds, backlayyesno, final_stack, final_exposure_pnl
    } = data;
    exposure = pnl = 0;
    if (backlayyesno) {
        exposure = final_exposure_pnl;
    } else {
        pnl = final_exposure_pnl;
    }
    postData = {
        MatchID: matchid,
        SportID: sportsid,
        AgentID: operatorkey,
        UserID: userid,
        MfancyID: (typeof mfancyid == 'undefined' ? "NA" : mfancyid),
        SelectionID: (typeof selectionid == 'undefined' ? "NA" : selectionid),
        MarketID: marketid,
        BetON: beton,
        BetOdds: odds,
        BackLayYesNo: backlayyesno,
        Staked: final_stack,
        Exposure: exposure,
        PNL: pnl
    }
    let apiKey = process.env.API_KEY_SAVE_BOX_CRICKET_BETS;
    axios.post(
        process.env.API_SAVE_BOX_CRICKET_BETS,
        postData,
        { headers: { "Content-Type": "application/json", APIkey: apiKey } },
    ).then(function (response) {
        //console.log("response", response.data);
    }).catch(({ response }) => {
        console.log("ERR : Got " + response.status + " in - " + response.config.url);
    })
    return;
}

async function insertBetMongodbselection(options = {}) {
    customLib.connectMongoDB();
    var cwin_closs = null;
    if (mongoose.models.cwin_closs) {
        cwin_closs = mongoose.model('cwin_closs');
    } else {
        cwin_closs = mongoose.model('cwin_closs', model_winLoss);
    }

    const {
        SportID, MatchID, MfancyID, MarketID,
        SelectionID, SelectionIDName, BetType, BetON,
        Linkedround, FancyReference, KillNo
    } = options;
    let GUID = uuid.v1();

    var record = {
        winlossid: GUID,
        sportsid: SportID,
        marketid: MarketID,
        matchid: MatchID,
        mfancyid: MfancyID,
        selectionid: SelectionID,
        win: 0,
        loss: 0,
        cwin: 0,
        closs: 0,
        endkill: 0,
        pnl: 0,
        calpnl: 0,
        markettype: BetType,
        createdby: "system"
    };
    if (BetType == 'YesNo') {
        record['beton'] = BetON
    }
    if (BetType == 'BacklayFancy') {
        record['killNo'] = KillNo
    }
    if (FancyReference != '') {
        record['fancyreference'] = FancyReference
    }
    if (SelectionIDName != '') {
        record['selectionidname'] = SelectionIDName
    }
    if (Linkedround != '') {
        record['linkedround'] = Linkedround
    }

    let insert_cwin_closs = new cwin_closs(record);

    return insert_cwin_closs.save().then(function (savedData) {
        return true;
    }).catch(function (err) {
        return false;
    });
}

async function initialEntryOfMatchodd(req, res) {
    var Bet = null;
    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }
    let betData = await Bet.find({ status: 1, is_pnl_calculated: 0, market_type: "MatchOdds" });
    if (betData.length > 0) {
        let tmpOddCache = {}
        let mongoCacheOdds = {}
        let cwin_closs = null
        if (mongoose.models.cwin_closs) {
            cwin_closs = mongoose.model('cwin_closs');
        } else {
            cwin_closs = mongoose.model('cwin_closs', model_winLoss);
        }
        for (const bet of betData) {
            let mongoCacheKey = bet.marketid + "_" + bet.matchid + "_" + bet.sportsid
            if (typeof mongoCacheOdds[mongoCacheKey] == 'undefined') {
                let cwindata = await cwin_closs.find({ marketid: bet.marketid, matchid: bet.matchid, sportsid: bet.sportsid });
                if (cwindata.length == 0) {
                    let tmpKey = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid
                    if (typeof tmpOddCache[tmpKey] == 'undefined') {
                        await new Promise((resolve, reject) => {
                            mysql_matchodds.getConnection(function (err, con) {
                                if (err) {
                                    realeaseSQLCon(con);
                                    return reject(false);
                                }
                                con.query(
                                    "SELECT * FROM matchodds WHERE SportsID = ? AND MatchID = ? AND MarketID = ?",
                                    [bet.sportsid, bet.matchid, bet.marketid],
                                    async function (err, result, fields) {
                                        con.release()
                                        if (err) {
                                            return reject(false);
                                        }
                                        let broadcastData = {}
                                        result.forEach((res, i) => {
                                            if (Object.keys(broadcastData).length <= 0) {
                                                broadcastData = {
                                                    "SportID": res.SportsID,
                                                    "MatchID": res.MatchID,
                                                    "MarketID": res.MarketID,
                                                    "Status": res.Status,
                                                    "StartDate": res.GameStartDate,
                                                    "TotalPlayer": 4,
                                                    "Team1": {},
                                                    "Team2": {},
                                                    "Team3": {},
                                                    "Team4": {},
                                                }
                                            }
                                            let TeamData = {}
                                            TeamData.PlayerID = res.PlayerID
                                            TeamData.PlayerName = res.PlayerName
                                            TeamData.Color = res.Color
                                            TeamData.SelectionID = res.SelectionID
                                            TeamData.Back = res.BackBhav
                                            TeamData.Lay = res.LayBhav

                                            if (res.Color == "Red") {
                                                broadcastData.Team1 = TeamData
                                            } else if (res.Color == "Blue") {
                                                broadcastData.Team2 = TeamData
                                            } else if (res.Color == "Yellow") {
                                                broadcastData.Team3 = TeamData
                                            } else if (res.Color == "Green") {
                                                broadcastData.Team4 = TeamData
                                            }
                                        });
                                        tmpOddCache[tmpKey] = broadcastData
                                        return resolve(broadcastData);
                                    }
                                );
                            })
                        })
                    }
                    let element = tmpOddCache[tmpKey]
                    if (typeof element != 'undefined') {
                        if (Object.keys(element).length > 0) {
                            let teamArr = [];
                            let totalPlayer = 4;
                            for (i = 1; i <= totalPlayer; i++) {
                                key = "Team" + i;
                                teamArr.push(element[key])
                            }
                            let insertData = {
                                SportID: element.SportID,
                                MatchID: element.MatchID,
                                MfancyID: "NA",
                                MarketID: element.MarketID,
                                SelectionID: 0,
                                SelectionIDName: "",
                                BetType: "MatchOdds",
                            }
                            for (let team of teamArr) {
                                insertData.SelectionID = team.SelectionID
                                insertData.SelectionIDName = team.Color
                                await insertBetMongodbselection(insertData)
                            }
                            await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                            }).clone();
                            mongoCacheOdds[mongoCacheKey] = 1
                        }
                    }
                } else {
                    await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                    }).clone();
                    mongoCacheOdds[mongoCacheKey] = 1
                }
            } else {
                await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                }).clone();
            }
        }
    }
}

async function initialEntryOfBacklay(req, res) {
    var Bet = null;
    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }
    let betData = await Bet.find({ status: 1, is_pnl_calculated: 0, market_type: "BacklayFancy" });
    if (betData.length > 0) {
        let tmpBacklayCache = {}
        let mongoCacheBacklay = {}
        let cwin_closs = null
        if (mongoose.models.cwin_closs) {
            cwin_closs = mongoose.model('cwin_closs');
        } else {
            cwin_closs = mongoose.model('cwin_closs', model_winLoss);
        }
        for (const bet of betData) {
            let mongoCacheKey = bet.marketid + "_" + bet.matchid + "_" + bet.sportsid
            if (typeof mongoCacheBacklay[mongoCacheKey] == 'undefined') {
                let cwindata = await cwin_closs.find({
                    marketid: bet.marketid,
                    matchid: bet.matchid,
                    sporstid: bet.sportsid
                });
                if (cwindata.length == 0) {
                    let tmpKey = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid
                    if (typeof tmpBacklayCache[tmpKey] == 'undefined') {
                        await new Promise((resolve, reject) => {
                            mysql_backlay.getConnection(function (err, con) {
                                if (err) {
                                    realeaseSQLCon(con);
                                    return reject(false);
                                }
                                con.query(
                                    "SELECT * FROM backlayfancylive WHERE SportsID = ? AND MatchID = ? AND MarketID = ?",
                                    [bet.sportsid, bet.matchid, bet.marketid],
                                    async function (err, result, fields) {
                                        con.release()
                                        if (err) {
                                            return reject(false);
                                        }
                                        tmpBacklayCache[tmpKey] = result
                                        return resolve(result);
                                    }
                                );
                            })
                        })
                    }
                    if (typeof tmpBacklayCache[tmpKey] != 'undefined') {
                        let data = tmpBacklayCache[tmpKey]
                        if (data.length > 0) {
                            for (let element of data) {
                                //if (typeof element.Status == "string" && element.Status.toUpperCase() == "LIVE" && element.MatchID == bet.matchid && element.MarketID == bet.marketid && element.SportsID == bet.sportsid) {
                                const {
                                    MatchID,
                                    MarketID,
                                    MFancyID,
                                    SportsID,
                                    FancyReference,
                                    KillNo
                                } = element;

                                insertData = {
                                    SportID: SportsID,
                                    MatchID: MatchID,
                                    MfancyID: MFancyID,
                                    MarketID: MarketID,
                                    SelectionID: MFancyID,
                                    SelectionIDName: "",
                                    FancyReference: FancyReference,
                                    BetType: "BacklayFancy",
                                    KillNo: KillNo
                                }
                                await insertBetMongodbselection(insertData)
                                await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                                }).clone();
                                //}
                            }
                            mongoCacheBacklay[mongoCacheKey] = 1
                        }
                    }
                } else {
                    await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                    }).clone();
                    mongoCacheBacklay[mongoCacheKey] = 1
                }
            } else {
                await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                }).clone();
            }
        }
    }
}

async function initialEntryOfSpecialfancy(req, res) {
    var Bet = null;
    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }
    let betData = await Bet.find({ status: 1, is_pnl_calculated: 0, market_type: "SpecialFancy" });
    if (betData.length > 0) {
        let tmpSpecialCache = {}
        let mongoCacheSpecial = {}
        let cwin_closs = null
        if (mongoose.models.cwin_closs) {
            cwin_closs = mongoose.model('cwin_closs');
        } else {
            cwin_closs = mongoose.model('cwin_closs', model_winLoss);
        }
        for (const bet of betData) {
            let mongoCacheKey = bet.marketid + "_" + bet.matchid + "_" + bet.sportsid + "_" + bet.mfancyid
            if (typeof mongoCacheSpecial[mongoCacheKey] == 'undefined') {
                let cwindata = await cwin_closs.find({
                    marketid: bet.marketid,
                    matchid: bet.matchid,
                    sporstid: bet.sportsid,
                    mfancyid: bet.mfancyid
                });
                if (cwindata.length == 0) {
                    let tmpKey = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid + "_" + bet.mfancyid
                    if (typeof tmpSpecialCache[tmpKey] == 'undefined') {
                        await new Promise((resolve, reject) => {
                            mysql_special.getConnection(function (err, con) {
                                if (err) {
                                    con.release()
                                    return reject(false);
                                }
                                con.query(
                                    "SELECT * FROM specialfancylive WHERE SportsID = ? AND MatchID = ? AND MarketID = ? AND MFancyID = ?",
                                    [bet.sportsid, bet.matchid, bet.marketid, bet.mfancyid],
                                    async function (err, result, fields) {
                                        con.release()
                                        if (err) {
                                            return reject(false);
                                        }
                                        tmpSpecialCache[tmpKey] = result
                                        return resolve(result);
                                    }
                                );
                            })
                        })
                    }
                    if (typeof tmpSpecialCache[tmpKey] != 'undefined') {
                        let data = tmpSpecialCache[tmpKey]
                        if (data.length > 0) {
                            for (let element of data) {
                                //if (typeof element.Status == "string" && element.MFancyID == bet.mfancyid && element.MatchID == bet.matchid && element.MarketID == bet.marketid && element.SportsID == bet.sportsid) {
                                const {
                                    MatchID, MarketID, MFancyID,
                                    Odds1, Odds2, Odds3, Odds4, Odds5, Odds6,
                                    Odds1SelectionID, Odds2SelectionID, Odds3SelectionID,
                                    Odds4SelectionID, Odds5SelectionID, Odds6SelectionID,
                                    SportsID, RoundLinked, FancyReference
                                } = element;

                                insertData = {
                                    SportID: SportsID,
                                    MatchID: MatchID,
                                    MfancyID: MFancyID,
                                    MarketID: MarketID,
                                    SelectionID: 0,
                                    SelectionIDName: "",
                                    Linkedround: RoundLinked,
                                    FancyReference: FancyReference,
                                    BetType: "SpecialFancy",
                                }
                                for (let teamCount = 0; teamCount < 6; teamCount++) {
                                    if (teamCount == 0) {
                                        insertData.SelectionID = Odds1SelectionID
                                        insertData.SelectionIDName = Odds1
                                    } else if (teamCount == 1) {
                                        insertData.SelectionID = Odds2SelectionID
                                        insertData.SelectionIDName = Odds2
                                    } else if (teamCount == 2) {
                                        insertData.SelectionID = Odds3SelectionID
                                        insertData.SelectionIDName = Odds3
                                    } else if (teamCount == 3) {
                                        insertData.SelectionID = Odds4SelectionID
                                        insertData.SelectionIDName = Odds4
                                    } else if (teamCount == 4) {
                                        insertData.SelectionID = Odds5SelectionID
                                        insertData.SelectionIDName = Odds5
                                    } else {
                                        insertData.SelectionID = Odds6SelectionID
                                        insertData.SelectionIDName = Odds6
                                    }
                                    await insertBetMongodbselection(insertData)
                                    await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                                    }).clone();
                                }
                                //}
                            }
                            mongoCacheSpecial[mongoCacheKey] = 1
                        }
                    }
                } else {
                    await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                    }).clone();
                    mongoCacheSpecial[mongoCacheKey] = 1
                }
            } else {
                await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                }).clone();
            }
        }
    }
}

async function initialEntryOfYesno(req, res) {
    var Bet = null;
    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }
    let betData = await Bet.find({ status: 1, is_pnl_calculated: 0, market_type: "YesNo" });

    if (betData.length > 0) {
        let tmpYesnoCache = {}
        let mongoCacheYesno = {}
        let cwin_closs = null
        if (mongoose.models.cwin_closs) {
            cwin_closs = mongoose.model('cwin_closs');
        } else {
            cwin_closs = mongoose.model('cwin_closs', model_winLoss);
        }
        for (const bet of betData) {
            let mongoCacheKey = bet.matchid + "_" + bet.fancy_reference + "_" + bet.round_linked + "_" + bet.beton
            if (typeof mongoCacheYesno[mongoCacheKey] == 'undefined') {
                let cwindata = await cwin_closs.find({
                    //marketid: bet.marketid,
                    //mfancyid: bet.mfancyid,
                    //sporstid: bet.sportsid,
                    matchid: bet.matchid,
                    fancyreference: bet.fancy_reference,
                    linkedround: bet.round_linked,
                    beton: bet.beton,
                    markettype: config.markettype.YESNO
                });
                if (cwindata.length == 0) {
                    let tmpKey = bet.sportsid + "_" + bet.matchid + "_" + bet.marketid + "_" + bet.mfancyid
                    if (typeof tmpYesnoCache[tmpKey] == 'undefined') {
                        await new Promise((resolve, reject) => {
                            mysql_yesno.getConnection(function (err, con) {
                                if (err) {
                                    con.release()
                                    return reject(false);
                                }
                                con.query(
                                    "SELECT * FROM yesnofancylive WHERE SportsID = ? AND MatchID = ? AND MarketID = ? AND MFancyID = ?",
                                    [bet.sportsid, bet.matchid, bet.marketid, bet.mfancyid],
                                    async function (err, result, fields) {
                                        con.release()
                                        if (err) {
                                            return reject(false);
                                        }
                                        tmpYesnoCache[tmpKey] = result
                                        return resolve(result);
                                    }
                                );
                            })
                        })
                    }
                    if (typeof tmpYesnoCache[tmpKey] != 'undefined') {
                        let data = tmpYesnoCache[tmpKey]
                        if (data.length > 0) {
                            for (let element of data) {
                                //if (typeof element.Status == "string" && element.Status.toUpperCase() == "LIVE" && element.MatchID == bet.matchid && element.MarketID == bet.marketid && element.MFancyID == bet.mfancyid && (element.Yes == bet.beton || element.No == bet.beton)) {
                                //if (element.MatchID == bet.matchid && element.MarketID == bet.marketid && element.MFancyID == bet.mfancyid) {
                                const {
                                    MatchID,
                                    MarketID,
                                    MFancyID,
                                    SportsID,
                                    Yes,
                                    No,
                                    RoundLinked,
                                    FancyReference
                                } = element;

                                let insertData = {
                                    SportID: SportsID,
                                    MatchID: MatchID,
                                    MfancyID: MFancyID,
                                    MarketID: MarketID,
                                    SelectionID: MFancyID,
                                    BetON: 0,
                                    BetType: "YesNo",
                                    Linkedround: RoundLinked,
                                    FancyReference: FancyReference,
                                }
                                if (bet.backlayyesno == 0) {
                                    insertData.BetON = Yes
                                    await insertBetMongodbselection(insertData)
                                } else {
                                    insertData.BetON = No
                                    await insertBetMongodbselection(insertData)
                                }
                                await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                                }).clone();
                                //}
                            }
                            mongoCacheYesno[mongoCacheKey] = 1
                        }
                    }
                } else {
                    await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                    }).clone();
                    mongoCacheYesno[mongoCacheKey] = 1
                }
            } else {
                await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 1 }, function (err, result) {
                }).clone();
            }
        }
    }
}

async function generateCwinClossEntries() {
    await Promise.all([
        initialEntryOfMatchodd(),
        initialEntryOfBacklay(),
        initialEntryOfSpecialfancy(),
        initialEntryOfYesno()
    ])
}

async function generatePNL(returnRes = false) {
    /* axios.get(
        process.env.API_PNL_CALCULATION,
        { headers: { "Content-Type": "application/json" } },
    ).then(function (response) {
        console.log("calculate PNl response : ", response.data);
    }).catch(({ response }) => {
        console.log("ERR : Got " + response.status + " in - " + response.config.url);
    })
    broadcastPNLUpdate();
    return; */

    await generateCwinClossEntries();

    var cwin_closs = null;
    var Bet = null;

    if (mongoose.models.cwin_closs) {
        cwin_closs = mongoose.model('cwin_closs');
    } else {
        cwin_closs = mongoose.model('cwin_closs', model_winLoss);
    }

    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }

    let betData = await Bet.find({ status: 1, is_pnl_calculated: 1 });
    //let cwindata = await cwin_closs.find({ linkedround: { $nin: [''] } });
    let cwindata = await cwin_closs.find();
    let winLossData = [];
    for (const bet of betData) {
        let API_Type = bet.market_type;
        /* let API_Type = "";
        await getMarketTypeFromMatchId(bet.marketid, bet.matchid).then(async function (matchTypeData) {
            API_Type = matchTypeData.API_Type;
        }); */
        if (winLossData.length) {
            let doQuery = true
            for (let groupId = 0; groupId < winLossData.length; groupId++) {
                let tmpData = winLossData[groupId]
                for (let entityId = 0; entityId < tmpWinLossData.length; entityId++) {
                    let winLoss = tmpData[entityId]
                    if (typeof winLoss != 'undefined') {
                        if (winLoss.marketid == bet.marketid && winLoss.matchid == bet.matchid && winLoss.sportsid == bet.sportsid) {
                            doQuery = false
                            break
                        }
                    }
                }
            }
            if (doQuery) {
                if (API_Type == config.markettype.SPECIAL) {
                    for (const cwin of cwindata) {
                        tmpWinLossData = await cwin_closs.find({
                            marketid: bet.marketid,
                            matchid: bet.matchid,
                            sportsid: bet.sportsid,
                            linkedround: cwin.linkedround
                        });
                        winLossData.push(tmpWinLossData);
                    }
                } else if (API_Type == config.markettype.YESNO) {
                    tmpWinLossData = await cwin_closs.find({
                        matchid: bet.matchid,
                        fancyreference: bet.fancy_reference,
                        linkedround: bet.round_linked,
                        beton: bet.beton,
                        markettype: 'YesNo'
                    });
                    winLossData.push(tmpWinLossData)
                } else {
                    tmpWinLossData = await cwin_closs.find({
                        marketid: bet.marketid,
                        matchid: bet.matchid,
                        sportsid: bet.sportsid
                    });
                    winLossData.push(tmpWinLossData);
                }
            }
        } else {
            if (API_Type == config.markettype.SPECIAL) {
                for (const cwin of cwindata) {
                    tmpWinLossData = await cwin_closs.find({
                        marketid: bet.marketid,
                        matchid: bet.matchid,
                        sportsid: bet.sportsid,
                        linkedround: cwin.linkedround
                    });
                    winLossData.push(tmpWinLossData);
                }
            } else if (API_Type == config.markettype.YESNO) {
                tmpWinLossData = await cwin_closs.find({
                    //marketid: bet.marketid,
                    //sportsid: bet.sportsid,
                    matchid: bet.matchid,
                    fancyreference: bet.fancy_reference,
                    linkedround: bet.round_linked,
                    beton: bet.beton,
                    markettype: 'YesNo'
                });
                winLossData.push(tmpWinLossData);
                /* for (const cwin of cwindata) {
                    tmpWinLossData = await cwin_closs.find({
                        //marketid: bet.marketid,
                        //sportsid: bet.sportsid,
                        matchid: bet.matchid,
                        fancyreference: bet.fancy_reference,
                        linkedround: bet.round_linked,
                        beton: bet.beton,
                        markettype: 'YesNo'
                    });
                    winLossData.push(tmpWinLossData);
                } */
            } else {
                tmpWinLossData = await cwin_closs.find({
                    marketid: bet.marketid,
                    matchid: bet.matchid,
                    sportsid: bet.sportsid
                });
                winLossData.push(tmpWinLossData);
            }
        }
        for (let groupId = 0; groupId < winLossData.length; groupId++) {
            let tmpWinLossData = winLossData[groupId]
            for (let entityId = 0; entityId < tmpWinLossData.length; entityId++) {
                let winLoss = tmpWinLossData[entityId]
                if (winLoss['markettype'] == config.markettype.YESNO) {
                    winLoss.selectionid = winLoss.beton
                    bet.selectionid = bet.beton
                }
                if (winLoss['markettype'] == config.markettype.BACKLAY) {
                    bet.selectionid = bet.mfancyid
                }
                if (winLoss.selectionid == bet.selectionid) {
                    if (bet['backlayyesno']) {
                        winLossData[groupId][entityId]['win'] = winLossData[groupId][entityId]['win'] - bet['final_exposure_pnl'];
                        winLossData[groupId][entityId]['loss'] = winLossData[groupId][entityId]['loss'] + bet['final_stack'];
                        winLossData[groupId][entityId]['cwin'] = (-1) * winLossData[groupId][entityId]['win'];
                        winLossData[groupId][entityId]['closs'] = (-1) * winLossData[groupId][entityId]['loss'];
                    } else {
                        winLossData[groupId][entityId]['win'] = winLossData[groupId][entityId]['win'] + bet['final_exposure_pnl'];
                        winLossData[groupId][entityId]['loss'] = winLossData[groupId][entityId]['loss'] - bet['final_stack'];
                        winLossData[groupId][entityId]['cwin'] = (-1) * winLossData[groupId][entityId]['win'];
                        winLossData[groupId][entityId]['closs'] = (-1) * winLossData[groupId][entityId]['loss'];
                    }
                }
            }
        }
        await Bet.updateOne({ guid: bet.guid }, { is_pnl_calculated: 2 }, function (err, result) { }).clone();
    }

    //Calculate pnl
    for (let groupId = 0; groupId < winLossData.length; groupId++) {
        let group = winLossData[groupId]
        for (let entityId = 0; entityId < group.length; entityId++) {
            let entity = group[entityId]
            if (entity['markettype'] == config.markettype.YESNO) {
                await calulateYesnoPNLOld(group)
                break;
            } else {
                pnlWin = entity['cwin']
                pnlLoss = 0;
                for (let tmpEntityId = 0; tmpEntityId < group.length; tmpEntityId++) {
                    let tmpEntity = group[tmpEntityId]
                    if (tmpEntity.selectionid != entity.selectionid) {
                        pnlLoss += tmpEntity['closs']
                    }
                }
                winLossData[groupId][entityId]['pnl'] = pnlWin + pnlLoss
                winLossData[groupId][entityId]['cpnl'] = (winLossData[groupId][entityId]['pnl']) / 10000
            }
        }
    }

    let response = []
    if (winLossData.length) {
        for (const group of winLossData) {
            if (group.length > 0) {
                pnl = '';
                for (const entity of group) {
                    await cwin_closs.updateOne({ winlossid: entity.winlossid }, {
                        win: entity['win'],
                        loss: entity['loss'],
                        cwin: entity['cwin'],
                        closs: entity['closs'],
                        pnl: parseFloat(entity['pnl'].toFixed(2)),
                        cpnl: parseFloat(entity['cpnl'].toFixed(2))
                    }, function (err, result) {
                    }).clone();
                    pnl += entity.selectionidname + "(" + entity['cpnl'] + "), "
                }
                response.push({
                    matchid: group[0]['matchid'],
                    pnl: pnl
                })
            }
        }
        updateMysqlForPnl()
    }
    await broadcastPNLUpdate();
    await broadcastPNLDetailsUpdate();
    if (returnRes) {
        return response;
    }
}

function padTo2Digits(num) {
    return num.toString().padStart(2, '0');
}
function formatDate(date) {
    return (
        [
            date.getFullYear(),
            padTo2Digits(date.getMonth() + 1),
            padTo2Digits(date.getDate()),
        ].join('-') +
        ' ' +
        [
            padTo2Digits(date.getHours()),
            padTo2Digits(date.getMinutes()),
            padTo2Digits(date.getSeconds()),
        ].join(':')
    );
}

async function updateMysqlForPnl() {
    var winLossModel = null;
    if (mongoose.models.cwin_closs) {
        winLossModel = mongoose.model('cwin_closs');
    } else {
        winLossModel = mongoose.model('cwin_closs', model_winLoss);
    }

    await winLossModel.find({ markettype: { $nin: ['YesNo'] } }).then(async function (result) {
        let groupTable = {
            'match_odds': {},
            'special': {},
            'backlay': {},
        }
        for (const resultElement of result) {
            if (resultElement['markettype'] == 'MatchOdds') {
                let matchid = resultElement['matchid']
                if (typeof groupTable['match_odds'][matchid] == 'undefined') {
                    groupTable['match_odds'][matchid] = []
                }
                groupTable['match_odds'][matchid].push(resultElement)
            } else if (resultElement['markettype'] == 'BacklayFancy') {
                let killNo = resultElement['killNo']
                let matchid = resultElement['matchid']
                if (typeof groupTable['backlay'][matchid] == 'undefined') {
                    groupTable['backlay'][matchid] = {}
                }
                if (typeof groupTable['backlay'][matchid][killNo] == 'undefined') {
                    groupTable['backlay'][matchid][killNo] = []
                }
                groupTable['backlay'][matchid][killNo].push(resultElement)
            } else if (resultElement['markettype'] == 'SpecialFancy') {
                let linkedRound = resultElement['linkedround']
                let matchid = resultElement['matchid']
                if (typeof groupTable['special'][matchid] == 'undefined') {
                    groupTable['special'][matchid] = {}
                }
                if (typeof groupTable['special'][matchid][linkedRound] == 'undefined') {
                    groupTable['special'][matchid][linkedRound] = []
                }
                groupTable['special'][matchid][linkedRound].push(resultElement)
            }
        }
        if (Object.keys(groupTable['special']).length > 0) {
            let sql = "INSERT INTO diceresult (ResultID, MatchID, Reference, Windata, RoundID, UpdatedDate) VALUES "
            for (const matchId in groupTable['special']) {
                for (const linkedRound in groupTable['special'][matchId]) {
                    let tmpSql = "('" + uuid.v1() + "','" + matchId + "',"
                    groupTable['special'][matchId][linkedRound] = groupTable['special'][matchId][linkedRound].sort(function (a, b) {
                        return a.pnl - b.pnl;
                    });
                    let tmpData = ''
                    for (const mData of groupTable['special'][matchId][linkedRound]) {
                        tmpData += mData['selectionidname'] + '(' + mData['cpnl'] + '),'
                    }
                    tmpData = tmpData.replace(/.$/, '')
                    tmpSql += "'Dice','" + tmpData + "','" + linkedRound + "','" + formatDate(new Date()) + "'),"
                    sql += tmpSql
                }
            }
            sql = sql.replace(/.$/, '')
            await new Promise((resolve, reject) => {
                mysql_pnl.getConnection(function (err, con) {
                    if (err) {
                        console.log("err", err)
                        realeaseSQLCon(con);
                        return reject(false);
                    }
                    con.query(
                        sql,
                        async function (err, result, fields) {
                            realeaseSQLCon(con);
                            if (err) {
                                console.log("Err", err)
                                return reject(false);
                            }
                            return resolve(result);
                        }
                    );
                });
            });
        }
        if (Object.keys(groupTable['backlay']).length > 0) {
            let sql = "INSERT INTO killresult (ResultID, MatchID, KillNo, Windata, UpdatedDate) VALUES "
            for (const matchId in groupTable['backlay']) {
                for (const killNo in groupTable['backlay'][matchId]) {
                    let tmpSql = "('" + uuid.v1() + "','" + matchId + "','" + killNo + "',"
                    groupTable['backlay'][matchId][killNo] = groupTable['backlay'][matchId][killNo].sort(function (a, b) {
                        return a.pnl - b.pnl;
                    });
                    let tmpData = ''
                    for (const mData of groupTable['backlay'][matchId][killNo]) {
                        tmpData += mData['fancyreference'] + '(' + mData['cpnl'] + '),'
                    }
                    tmpData = tmpData.replace(/.$/, '')
                    tmpSql += "'" + tmpData + "','" + formatDate(new Date()) + "'),"
                    sql += tmpSql
                }
            }
            sql = sql.replace(/.$/, '')
            await new Promise((resolve, reject) => {
                mysql_pnl.getConnection(function (err, con) {
                    if (err) {
                        console.log("err", err)
                        realeaseSQLCon(con);
                        return reject(false);
                    }
                    con.query(
                        sql,
                        async function (err, result, fields) {
                            realeaseSQLCon(con);
                            if (err) {
                                console.log("Err", err)
                                return reject(false);
                            }
                            return resolve(result);
                        }
                    );
                });
            });
        }
        if (Object.keys(groupTable['match_odds']).length > 0) {
            let saveData = {}
            for (const matchId in groupTable['match_odds']) {
                groupTable['match_odds'][matchId] = groupTable['match_odds'][matchId].sort(function (a, b) {
                    return a.pnl - b.pnl;
                });
                for (const mData of groupTable['match_odds'][matchId]) {
                    let mId = mData['matchid']
                    if (typeof saveData[mId] == 'undefined') {
                        saveData[mId] = {
                            'ResultID': uuid.v1(),
                            'MatchID': mId,
                            'Reference': '1st winner',
                            'Windata': '',
                            'Todecreased': '',
                            'ToIncreaed': '',
                            'UpdatedDate': formatDate(new Date())
                        }
                    }
                    saveData[mId]['Windata'] += mData['selectionidname'] + '(' + mData['cpnl'] + '),'
                }
                saveData[matchId]['Todecreased'] = groupTable['match_odds'][matchId][0]['selectionidname'] + ', ' + groupTable['match_odds'][matchId][1]['selectionidname']
                saveData[matchId]['ToIncreaed'] = groupTable['match_odds'][matchId][3]['selectionidname'] + ', ' + groupTable['match_odds'][matchId][2]['selectionidname']
            }
            if (Object.keys(saveData).length > 0) {
                let sql = "INSERT INTO ludoresult (ResultID, MatchID, Reference, Windata, Todecreased, ToIncreaed, UpdatedDate) VALUES "
                for (const matchId in saveData) {
                    let tmpData = saveData[matchId]
                    sql += '("' + tmpData['ResultID'] + '","' + tmpData['MatchID'] + '","' + tmpData['Reference'] + '","' + tmpData['Windata'] + '","' + tmpData['Todecreased'] + '","' + tmpData['ToIncreaed'] + '","' + tmpData['UpdatedDate'] + '"),'
                }
                sql = sql.replace(/.$/, '')
                sql += ';'
                await new Promise((resolve, reject) => {
                    mysql_pnl.getConnection(function (err, con) {
                        if (err) {
                            console.log("err", err)
                            realeaseSQLCon(con);
                            return reject(false);
                        }
                        con.query(
                            sql,
                            async function (err, result, fields) {
                                realeaseSQLCon(con);
                                if (err) {
                                    console.log("Err", err)
                                    return reject(false);
                                }
                                return resolve(result);
                            }
                        );
                    });
                });
            }
        }
    });

    var yesnoPnlModel = null;
    if (mongoose.models.yesno) {
        yesnoPnlModel = mongoose.model('yesNOPNL');
    } else {
        yesnoPnlModel = mongoose.model('yesNOPNL', model_yesno);
    }

    await yesnoPnlModel.find({}).then(async function (result) {
        if (result.length > 0) {
            yesNoData = {}
            for (const resultElement of result) {
                let mId = resultElement['matchid']
                let linkedRound = resultElement['linkedround']
                let fancyReference = resultElement['fancyreference']
                if (typeof yesNoData[mId] == 'undefined') {
                    yesNoData[mId] = {}
                }
                if (typeof yesNoData[mId][linkedRound] == 'undefined') {
                    yesNoData[mId][linkedRound] = {}
                }
                if (typeof yesNoData[mId][linkedRound][fancyReference] == 'undefined') {
                    yesNoData[mId][linkedRound][fancyReference] = ''
                }
                yesNoData[mId][linkedRound][fancyReference] += resultElement['beton'] + '(' + resultElement['cpnl'].toFixed(2) + '),'
            }
            if (Object.keys(yesNoData).length > 0) {
                let sql = "INSERT INTO diceresult (ResultID, MatchID, Reference, Windata, RoundID, UpdatedDate) VALUES "
                for (const mId in yesNoData) {
                    for (const linkedRound in yesNoData[mId]) {
                        for (const fancyReference in yesNoData[mId][linkedRound]) {
                            windata = yesNoData[mId][linkedRound][fancyReference].replace(/.$/, '')
                            sql += "('" + uuid.v1() + "','" + mId + "','" + fancyReference + "','" + (yesNoData[mId][linkedRound][fancyReference].replace(/.$/, '')) + "'," + linkedRound + ",'" + formatDate(new Date()) + "'),"
                        }
                    }
                }
                sql = sql.replace(/.$/, '')
                await new Promise((resolve, reject) => {
                    mysql_pnl.getConnection(function (err, con) {
                        if (err) {
                            console.log("err", err)
                            realeaseSQLCon(con);
                            return reject(false);
                        }
                        con.query(
                            sql,
                            async function (err, result, fields) {
                                realeaseSQLCon(con);
                                if (err) {
                                    console.log("Err", err)
                                    return reject(false);
                                }
                                return resolve(result);
                            }
                        );
                    });
                });
            }
        }
    })
}

async function verifyMatchOdd(req, res) {
    const { matchId, selID, markID, BackLayYesNo, BetOdds, Staked, UserID, ParentID, mFancyID, OperatorID, SportId, Currency, checksum, user_ip } = customLib.cleanTheBody(req.body);
    if (typeof matchOddCache[matchId] == 'undefined') {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    let data = matchOddCache[matchId];
    matchOddData = null
    matchFoundButSuspended = false
    let matchStatus = (typeof data.Status == "string") ? data.Status.toUpperCase() : '';
    if (matchStatus == "LIVE" || matchStatus == "SUSPENDED") {
        const { MatchID, MarketID, SportID } = customLib.cleanTheBody(data);
        if (MarketID == markID && MatchID == matchId && SportID == SportId) {
            if (matchStatus == "LIVE") {
                matchOddData = data
            } else if (matchStatus == "SUSPENDED") {
                matchFoundButSuspended = true
            }
        }
    }
    if (matchFoundButSuspended) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 11,
            errorDescription: config.errCode[11],
            checksum: checksum
        });
        return;
    }
    if (matchOddData == null) {
        console.log("ERR : No matchodd data found for verification");
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    try {
        let BetOdds_final = BetOdds;
        let MatchOddsFiltered = [null, null, null];
        let totalPlayer = matchOddData.TotalPlayer;
        let teamArr = [];
        const { MatchID } = matchOddData

        for (i = 1; i <= totalPlayer; i++) {
            key = "Team" + i;
            teamArr.push(matchOddData[key])
        }

        teamArr.forEach(team => {
            if (team.SelectionID == selID) {
                MatchOddsFiltered = [team.Lay, team.Back, team.Color];
            }
        })

        if (MatchOddsFiltered.includes(null)) {
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: 9,
                errorDescription: config.errCode[9],
                checksum: checksum
            });
            return;
        } else {
            let GUID = uuid.v1();
            insertData = {
                BetType: 1,
                MatchID: MatchID,
                Stake: parseFloat(Staked),
                UserID: UserID,
                parentID: ParentID,
                BetOdds: BetOdds_final,
                IsMatched: 0,
                BetON: MatchOddsFiltered[2],
                MarketID: markID,
                MfancyID: mFancyID,
                SelectionID: selID,
                OperatorID: OperatorID,
                SportID: SportId,
                BackLayYesNo: BackLayYesNo,
                Currency: Currency,
                Exposure_pnl: 0,
                Status: 0,
                GUID: GUID,
                fancy_family: '',
                fancy_name: '',
                fancy_reference: '',
                ip_address: user_ip,
                market_type: "MatchOdds"
            }

            stackFromDB = getStackCacheData('match_odds', Currency);
            if (stackFromDB == false) {
                res.send({
                    status: true,
                    OperatorID: OperatorID,
                    userId: UserID,
                    errorCode: 9,
                    errorDescription: config.errCode[9],
                    checksum: checksum
                });
                return;
            }

            returnStatus = 1
            if (Staked >= stackFromDB.min_value && Staked <= stackFromDB.max_value) {
                apiOdds = null
                if (BackLayYesNo == 0) {
                    apiOdds = MatchOddsFiltered[1];
                } else {
                    apiOdds = MatchOddsFiltered[0];
                }
                if (customLib.checkMeOut(BackLayYesNo, BetOdds, apiOdds)) {
                    BetOdds_final = apiOdds
                    returnStatus = 1;
                } else {
                    returnStatus = 5;
                }
            } else {
                returnStatus = 7;
            }

            let exposure_pnl = (BetOdds_final - 1) * Staked;
            insertData["BetOdds"] = BetOdds_final;
            insertData["Exposure_pnl"] = exposure_pnl.toFixed(config.decimalPlace);
            insertData["Status"] = returnStatus;
            response = await insertBetMongodb(insertData);
            if (response == false) {
                returnStatus = 9;
            }
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: returnStatus,
                errorDescription: config.errCode[returnStatus],
                guid: GUID,
                data: insertData,
                checksum: checksum
            });
            return;
        }
    } catch (e) {
        console.log("ERR : Error while verifying match odds");
        console.log(e);
        res.send({
            status: false,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 9,
            errorDescription: config.errCode[9],
            checksum: checksum
        });
        return
    }
}

async function verifyBackLayFancy(req, res) {
    const { selID, matchId, markID, BackLayYesNo, BetOdds, Staked, UserID, ParentID, mFancyID, OperatorID, SportId, Currency, checksum, user_ip } = customLib.cleanTheBody(req.body);
    if (typeof matchBacklayCache[matchId] == 'undefined') {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }
    backLayFancyData = null
    matchFoundButSuspended = false
    let data = matchBacklayCache[matchId];

    data.forEach(element => {
        let matchStatus = (typeof element.Status == "string") ? element.Status.toUpperCase() : ''
        if (matchStatus == "LIVE" || matchStatus == "SUSPENDED") {
            const { MatchID, MarketID, SportsID, MFancyID } = customLib.cleanTheBody(element);
            if (MarketID == markID && MatchID == matchId && SportsID == SportId && MFancyID == mFancyID) {
                if (matchStatus == "LIVE") {
                    backLayFancyData = element
                    return
                } else if (matchStatus == "SUSPENDED") {
                    matchFoundButSuspended = true
                    return
                }
            }
        }
    })
    if (matchFoundButSuspended) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 11,
            errorDescription: config.errCode[11],
            checksum: checksum
        });
        return;
    }
    if (backLayFancyData == null) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 11,
            errorDescription: config.errCode[11],
            checksum: checksum
        });
        return;
    }
    if (backLayFancyData == null) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    try {
        let BetOdds_final = BetOdds;
        const {
            MatchID,
            MFancyID,
            FancyName,
            FancyFamily,
            Back,
            Lay,
            MinBet,
            MaxBet,
            Status,
            selection_ID,
            FancyReference
        } = backLayFancyData;
        let verified = false
        if (MFancyID == mFancyID) {
            verified = true
            if (selection_ID && selection_ID != selID) {
                verified = false
            }
        }

        if (verified) {
            let GUID = uuid.v1();
            insertData = {
                BetType: 1,
                MatchID: matchId,
                Stake: parseFloat(Staked),
                UserID: UserID,
                parentID: ParentID,
                BetOdds: BetOdds_final,
                IsMatched: 0,
                BetON: FancyReference,
                MarketID: markID,
                MfancyID: mFancyID,
                SelectionID: selID,
                OperatorID: OperatorID,
                SportID: SportId,
                BackLayYesNo: BackLayYesNo,
                Currency: Currency,
                Exposure_pnl: 0,
                Status: 0,
                GUID: GUID,
                fancy_family: FancyFamily,
                fancy_name: FancyName,
                fancy_reference: FancyReference,
                ip_address: user_ip,
                market_type: "BacklayFancy"
            };

            stackFromDB = getStackCacheData('backlay', Currency);
            if (stackFromDB == false) {
                res.send({
                    status: true,
                    OperatorID: OperatorID,
                    userId: UserID,
                    errorCode: 9,
                    errorDescription: config.errCode[9],
                    checksum: checksum
                });
                return;
            }
            returnStatus = 1
            if (Staked >= stackFromDB.min_value && Staked <= stackFromDB.max_value) {
                apiOdds = null
                if (BackLayYesNo == 0) {
                    if (Staked >= MinBet && Staked <= MaxBet) {
                        BetOdds_final = Back;
                        returnStatus = 1;
                    } else {
                        returnStatus = 7;
                    }
                } else {
                    if (Staked >= MinBet && Staked <= MaxBet) {
                        BetOdds_final = Lay;
                        returnStatus = 1;
                    } else {
                        returnStatus = 7;
                    }
                }
            } else {
                returnStatus = 7;
            }

            let exposure_pnl = (BetOdds_final - 1) * Staked;
            insertData["BetOdds"] = BetOdds_final;
            insertData["Exposure_pnl"] = exposure_pnl.toFixed(config.decimalPlace);
            insertData["Status"] = returnStatus;
            response = await insertBetMongodb(insertData, backLayFancyData);
            if (response == false) {
                returnStatus = 9;
            }
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: returnStatus,
                errorDescription: config.errCode[returnStatus],
                guid: GUID,
                data: insertData,
                checksum: checksum
            });
            return;
        } else {
            res.send({
                status: false,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: 3,
                errorDescription: config.errCode[3],
                checksum: checksum
            });
            return;
        }
    } catch (e) {
        console.log("ERR : Error while verifying backlayfancy");
        console.log(e);
        res.send({
            status: false,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 9,
            errorDescription: config.errCode[9],
            checksum: checksum
        });
        return
    }
}

async function verifySpecialFancy(req, res) {
    const { selID, matchId, markID, BackLayYesNo, BetOdds, Staked, UserID, ParentID, mFancyID, OperatorID, SportId, Currency, BetOn, checksum, user_ip } = customLib.cleanTheBody(req.body);

    if (typeof matchSpecialCache[matchId] == 'undefined') {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    specialFancyData = null
    matchFoundButSuspended = false
    let data = matchSpecialCache[matchId];
    data.forEach(element => {
        let matchStatus = (typeof element.Status == "string") ? element.Status.toUpperCase() : ''
        if (matchStatus == "LIVE" || matchStatus == "SUSPENDED") {
            const { MatchID, MarketID, SportsID, MFancyID } = customLib.cleanTheBody(element);
            if (MarketID == markID && MatchID == matchId && SportsID == SportId && MFancyID == mFancyID) {
                if (matchStatus == "LIVE") {
                    specialFancyData = element
                    return
                } else if (matchStatus == "SUSPENDED") {
                    matchFoundButSuspended = true
                    return
                }
            }
        }
    })

    if (matchFoundButSuspended) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 11,
            errorDescription: config.errCode[11],
            checksum: checksum
        });
        return;
    }

    if (specialFancyData == null) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    try {
        let BetOdds_final = BetOdds;
        const {
            MatchID, MFancyID, FancyName, FancyType, Odds1, Odds1back, Odds1Lay, Odds2,
            Odds2back, Odds2Lay, Odds3, Odds3back, Odds3Lay, Odds4, Odds4back,
            Odds4Lay, Odds5, Odds5back, Odds5Lay, Odds6, Odds6back, Odds6Lay,
            Odds1SelectionID, Odds2SelectionID, Odds3SelectionID, Odds4SelectionID,
            Odds5SelectionID, Odds6SelectionID, FancyReference
        } = specialFancyData;

        let oddsFiltered = [null, null, null];
        if (MFancyID == mFancyID) {
            let GUID = uuid.v1();
            oddsFiltered = selID == Odds1SelectionID ? [Odds1, Odds1back, Odds1Lay]
                : selID == Odds2SelectionID ? [Odds2, Odds2back, Odds2Lay]
                    : selID == Odds3SelectionID ? [Odds3, Odds3back, Odds3Lay]
                        : selID == Odds4SelectionID ? [Odds4, Odds4back, Odds4Lay]
                            : selID == Odds5SelectionID ? [Odds5, Odds5back, Odds5Lay]
                                : selID == Odds6SelectionID ? [Odds6, Odds6back, Odds6Lay]
                                    : oddsFiltered;
            insertData = {
                BetType: 3,
                MatchID: MatchID,
                Stake: parseFloat(Staked),
                UserID: UserID,
                parentID: ParentID,
                BetOdds: BetOdds_final,
                IsMatched: 0,
                BetON: oddsFiltered[0],
                MarketID: markID,
                MfancyID: MFancyID,
                SelectionID: selID,
                OperatorID: OperatorID,
                SportID: SportId,
                BackLayYesNo: BackLayYesNo,
                Currency: Currency,
                Exposure_pnl: 0,
                Status: 0,
                GUID: GUID,
                fancy_family: FancyType,
                fancy_name: FancyName,
                fancy_reference: FancyReference,
                ip_address: user_ip,
                market_type: "SpecialFancy"
            };

            stackFromDB = getStackCacheData('special', Currency);
            if (stackFromDB == false) {
                res.send({
                    status: true,
                    OperatorID: OperatorID,
                    userId: UserID,
                    errorCode: 9,
                    errorDescription: config.errCode[9],
                    checksum: checksum
                });
                return;
            }

            returnStatus = 1
            if (Staked >= stackFromDB.min_value && Staked <= stackFromDB.max_value) {
                if (BackLayYesNo == 0) {
                    if (oddsFiltered[0] == BetOn) {
                        BetOdds_final = oddsFiltered[1];
                        returnStatus = 1
                    } else {
                        returnStatus = 5
                    }
                } else {
                    if (oddsFiltered[0] == BetOn) {
                        BetOdds_final = oddsFiltered[2];
                        returnStatus = 1
                    } else {
                        returnStatus = 5
                    }
                }
            } else {
                returnStatus = 7;
            }

            let exposure_pnl = (BetOdds_final / 100) * Staked;

            insertData["BetOdds"] = BetOdds_final;
            insertData["Exposure_pnl"] = exposure_pnl.toFixed(config.decimalPlace);
            insertData["Status"] = returnStatus;
            response = await insertBetMongodb(insertData, specialFancyData);
            if (response == false) {
                returnStatus = 9;
            }
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: returnStatus,
                errorDescription: config.errCode[returnStatus],
                guid: GUID,
                data: insertData,
                checksum: checksum
            });
            return;
        } else {
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: 3,
                errorDescription: config.errCode[3],
                checksum: checksum
            });
            return;
        }
    } catch (e) {
        console.log("ERR : Error while verifying specialfancy");
        console.log(e);
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 9,
            errorDescription: config.errCode[9],
            checksum: checksum
        });
        return;
    }
}

async function verifyYesNo(req, res) {
    const { selID, matchId, markID, BackLayYesNo, BetOdds, Staked, UserID, ParentID, mFancyID, OperatorID, SportId, Currency, BetOn, checksum, user_ip } = customLib.cleanTheBody(req.body);
    if (typeof matchYesnoCache[matchId] == 'undefined') {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    yesNoFancyData = null
    matchFoundButSuspended = false
    let data = matchYesnoCache[matchId];
    data.forEach(element => {
        let matchStatus = (typeof element.Status == "string") ? element.Status.toUpperCase() : ''
        if (matchStatus == "LIVE" || matchStatus == "SUSPENDED") {
            const { MatchID, MarketID, SportsID, MFancyID } = customLib.cleanTheBody(element);
            if (MarketID == markID && MatchID == matchId && SportsID == SportId && MFancyID == mFancyID) {
                if (matchStatus == "LIVE") {
                    yesNoFancyData = element
                    return
                } else if (matchStatus == "SUSPENDED") {
                    matchFoundButSuspended = true
                    return
                }
            }
        }
    })

    if (matchFoundButSuspended) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 11,
            errorDescription: config.errCode[11],
            checksum: checksum
        });
        return;
    }

    if (yesNoFancyData == null) {
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 3,
            errorDescription: config.errCode[3],
            checksum: checksum
        });
        return;
    }

    try {
        let BetOdds_final = BetOdds;
        const {
            MFancyID,
            No,
            Yes,
            NoOdds,
            YesOdds,
            MinBet,
            MaxBet,
            selection_ID,
            FancyType,
            FancyName,
            FancyReference,
            RoundLinked
        } = yesNoFancyData;
        let yesnoFiltered = [null, null, null];
        yesnoFiltered = BackLayYesNo == 0 ? [Yes, YesOdds]
            : BackLayYesNo == 1 ? [No, NoOdds]
                : yesnoFiltered;
        let verified = false
        if (MFancyID == mFancyID) {
            verified = true
            if (selection_ID && selection_ID != selID) {
                verified = false
            }
        }

        if (verified) {
            let GUID = uuid.v1();
            insertData = {
                BetType: 2,
                MatchID: matchId,
                Stake: parseFloat(Staked),
                UserID: UserID,
                parentID: ParentID,
                BetOdds: 0,
                IsMatched: 0,
                BetON: 0,
                MarketID: markID,
                MfancyID: mFancyID,
                SelectionID: selID,
                OperatorID: OperatorID,
                SportID: SportId,
                BackLayYesNo: BackLayYesNo,
                Currency: Currency,
                Exposure_pnl: 0,
                Status: 0,
                GUID: GUID,
                fancy_family: FancyType,
                fancy_name: FancyName,
                fancy_reference: FancyReference,
                ip_address: user_ip,
                market_type: "YesNo",
                RoundLinked: RoundLinked
            }

            stackFromDB = getStackCacheData('yesno', Currency);
            if (stackFromDB == false) {
                res.send({
                    status: true,
                    OperatorID: OperatorID,
                    userId: UserID,
                    errorCode: 9,
                    errorDescription: config.errCode[9],
                    checksum: checksum
                });
                return;
            }

            returnStatus = 1
            betOn = BetOn
            if (Staked >= stackFromDB.min_value && Staked <= stackFromDB.max_value) {
                if (BetOn == yesnoFiltered[0]) {
                    BetOdds_final = yesnoFiltered[1];
                    betOn = yesnoFiltered[0]
                    if (Staked >= MinBet && Staked <= MaxBet) {
                        returnStatus = 1
                    } else {
                        returnStatus = 7
                    }
                } else {
                    if (Staked >= MinBet && Staked <= MaxBet) {
                        returnStatus = 5
                    } else {
                        returnStatus = 7
                    }
                }
            } else {
                returnStatus = 7
            }

            let exposure_pnl = (BetOdds_final / 100) * Staked;
            insertData["BetOdds"] = BetOdds_final;
            insertData["Exposure_pnl"] = exposure_pnl.toFixed(config.decimalPlace);
            insertData["Status"] = returnStatus;
            insertData["BetON"] = betOn;
            response = await insertBetMongodb(insertData, yesNoFancyData);
            if (response == false) {
                returnStatus = 9;
            }
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: returnStatus,
                errorDescription: config.errCode[returnStatus],
                guid: GUID,
                data: insertData,
                checksum: checksum
            });
            return;
        } else {
            res.send({
                status: true,
                OperatorID: OperatorID,
                userId: UserID,
                errorCode: 3,
                errorDescription: config.errCode[3],
                checksum: checksum
            });
            return;
        }
    } catch (e) {
        console.log("ERR : Error while verifying yesno");
        console.log(e);
        res.send({
            status: true,
            OperatorID: OperatorID,
            userId: UserID,
            errorCode: 9,
            errorDescription: config.errCode[9],
            checksum: checksum
        });
        return;
    }
}

async function calulateYesnoPNLOld(group) {
    if (mongoose.models.yesno) {
        yesnoPnlModel = mongoose.model('yesNOPNL');
    } else {
        yesnoPnlModel = mongoose.model('yesNOPNL', model_yesno);
    }
    let filters = {
        sportsid: group[0].sportsid,
        matchid: group[0].matchid,
        marketid: group[0].marketid,
        mfancyid: group[0].mfancyid,
        linkedround: group[0].linkedround
    }
    let cwinclossYesNo = await yesnoPnlModel.find(filters);
    yesNoPnl = {}
    let deleteOldEntries = false
    if (typeof cwinclossYesNo != 'undefined' && cwinclossYesNo.length > 0) {
        deleteOldEntries = true
        for (const yesno of cwinclossYesNo) {
            yesNoPnl[yesno.beton] = yesno.pnl
        }
    }

    const beton = parseInt(group[0].beton)
    const min = beton - 1;
    const max = beton + 1;

    let arrLen = Object.keys(yesNoPnl).length
    if (arrLen <= 0) {
        for (let i = min; i <= max; i++) {
            yesNoPnl[i] = 0
        }
    } else {
        for (let i = min; i <= max; i++) {
            if (typeof yesNoPnl[i] == 'undefined' || yesNoPnl[i] == null) {
                let len = Object.keys(yesNoPnl).length
                let firstKey = Object.keys(yesNoPnl)[0];
                let firstValue = yesNoPnl[Object.keys(yesNoPnl)[0]];
                let lastKey = Object.keys(yesNoPnl)[len - 1];
                let lastValue = yesNoPnl[Object.keys(yesNoPnl)[len - 1]];
                if (i > lastKey) {
                    yesNoPnl[i] = lastValue
                } else if (i < firstKey) {
                    yesNoPnl[i] = firstValue
                } else {
                    for (let j = 0; j <= len; j++) {
                        let tmpVal = Object.keys(yesNoPnl)[j]
                        if (i < tmpVal) {
                            yesNoPnl[i] = yesNoPnl[tmpVal]
                            break
                        }
                    }
                }
            }
        }
    }

    yesNoPnl = Object.keys(yesNoPnl).sort().reduce(
        (obj, k) => {
            obj[k] = yesNoPnl[k];
            return obj;
        },
        {}
    );

    let len = Object.keys(yesNoPnl).length
    let firstKey = Object.keys(yesNoPnl)[0];
    let lastLast = Object.keys(yesNoPnl)[len - 1];
    for (let i = firstKey; i <= lastLast; i++) {
        if (typeof yesNoPnl[i] == 'undefined' || yesNoPnl[i] == null) {
            let len1 = Object.keys(yesNoPnl).length
            for (let j = 0; j <= len1; j++) {
                let tmpVal = Object.keys(yesNoPnl)[j]
                if (i < tmpVal) {
                    yesNoPnl[i] = yesNoPnl[tmpVal]
                    break
                }
            }
        }
    }

    for (const k in yesNoPnl) {
        if (k < group[0].beton) {
            yesNoPnl[k] = yesNoPnl[k] + group[0]["closs"]
        } else {
            yesNoPnl[k] = yesNoPnl[k] + group[0]["cwin"]
        }
    }

    if (deleteOldEntries) {
        await yesnoPnlModel.deleteMany(filters);
    }

    insertYesnoData = []
    for (const k in yesNoPnl) {
        let insertData = {
            sportsid: group[0].sportsid,
            matchid: group[0].matchid,
            marketid: group[0].marketid,
            mfancyid: group[0].mfancyid,
            beton: k,
            linkedround: group[0].linkedround,
            fancyreference: group[0].fancyreference,
            pnl: yesNoPnl[k],
            cpnl: (yesNoPnl[k]) / 10000,
            markettype: "YesNo",
        }
        insertYesnoData.push(insertData)
    }
    await yesnoPnlModel.insertMany(insertYesnoData)

    // 
    /* console.log("group",group)
    if (mongoose.models.yesno) {
        yesno = mongoose.model('yesNOPNL');
    } else {
        yesno = mongoose.model('yesNOPNL', model_yesno);
    }
    let testarray = [];
    for (let i = 0; i < group.length; i++) {
        const element = group[i];
        testarray.push(element['beton']);
    }
    const max = Math.max.apply(Math, testarray) + 1;
    const min = Math.min.apply(Math, testarray) - 1;

    for (let id = 0; id < group.length; id++) {
        const element = group[id];
        for (let i = min; i <= max; i++) {
            let yesnodata = await yesno.find({ beton: i });
            if (yesnodata.length == 0) {
                insertData = {
                    SportID: element['sportsid'],
                    MatchID: element['matchid'],
                    MfancyID: element['mfancyid'],
                    MarketID: element['marketid'],
                    fancyreference: element['fancyreference'],
                    BetON: i,
                    LinkedRound: element['linkedround'],
                }
                await updateBetMongodbyesno(insertData);
            }
        }
    }

    for (let pnl = 0; pnl < group.length; pnl++) {
        const element = group[pnl];

        data = await yesno.find({
            matchid: element.matchid,
            fancyreference: element.fancyreference,
            linkedround: element.linkedround
        });

        for (let i of data) {
            if (element['beton'] < i.beton) {
                i.pnl = i.pnl + element['cwin']
            } else {
                i.pnl = i.pnl + element['closs']
            }
            i.cpnl = i.pnl / 10000;
            await yesno.updateMany({ beton: i.beton }, { pnl: i.pnl, cpnl: i.cpnl }, function (err, result) {
            }).clone();
        }
    } */
}

async function stackDataCache() {
    if (mongoose.models.stack_data) {
        stack_data = mongoose.model('stack_data');
    } else {
        stack_data = mongoose.model('stack_data', model_stack_data);

        stack_data.insertMany(default_stack_data).then((docs) => {
            //console.log("Default stack data has been Saved to collection.");
        }).catch((err) => {
            return console.error(err);
        })
    }

    let stackData = await stack_data.find();
    if (stackData.length != 0) {
        stackData.forEach(res => {
            const { type, min_value, max_value, currency } = res
            cacheStackData["stack-" + type + "-" + currency] = {
                "type": type,
                "min_value": min_value,
                "max_value": max_value,
                "currency": currency
            }
        })
        console.log("Stack data cached sucessfully");
    }
}

async function apiKeyVerification(operatorKey = "", requestApiKey = "") {
    try {
        if (typeof requestApiKey == 'undefined') {
            return false
        }
        let cacheData = await getTblAdminData(operatorKey)
        if (!cacheData) {
            return false;
        }
        const { api_key } = cacheData
        if (api_key != requestApiKey) {
            return false;
        }
        return true
    } catch (e) {
        return false
    }
}

async function ip_verification(ips = "", operatorKey = "") {
    cacheData = await getTblAdminData(operatorKey)
    if (!cacheData) {
        return false;
    }
    const { IPwhitelist, api_key } = cacheData
    if (IPwhitelist == '' || api_key == '') {
        return false;
    }
    let ipWhiteList = IPwhitelist.split(",");
    result = false;
    for (let ip of ipWhiteList) {
        ip = ip.replace(' ', '');
        if (ip == ips) {
            return true;
        }
    }
    return false;
}

function cacheTblAdminDataGenerate() {
    console.log("cacheTblAdminData initialize");
    mysql_accounting.getConnection(function (err, con) {
        if (err) {
            console.log("err", err);
            realeaseSQLCon(con);
            return false;
        }
        con.query(
            "SELECT operatorkey, multiple_factor, currency, IPwhitelist, api_key FROM `tbladmin` WHERE `status`=?",
            [1],
            async function (err, result, fields) {
                if (err) {
                    console.log("Error occured while fetching operators for cache");
                    console.log(err);
                    realeaseSQLCon(con);
                } else {
                    console.log('---Operator Caching Start---');
                    result.forEach(res => {
                        const { operatorkey, currency, multiple_factor, IPwhitelist, api_key } = res
                        cacheTblAdminData["opearator-" + operatorkey] = {
                            "currency": currency,
                            "multiple_factor": multiple_factor,
                            "IPwhitelist": IPwhitelist,
                            "api_key": api_key,
                        }
                    })
                    console.log("Data cached sucessfully");
                }
            })
    })
}

async function getTblAdminData(operatorkey = "") {
    if (typeof cacheTblAdminData["opearator-" + operatorkey] !== 'undefined') {
        return cacheTblAdminData["opearator-" + operatorkey]
    } else {
        return false
    }
}

function getStackCacheData(type = "", currency = "") {
    if (typeof cacheStackData["stack-" + type + "-" + currency] != 'undefined') {
        return cacheStackData["stack-" + type + "-" + currency]
    }
    return false
}

async function getMarketTypeFromMatchId(marketId = "", matchId = "") {
    try {
        let cacheKey = "match_market_map_" + matchId + '-' + marketId;
        if (typeof cacheMarketTypeFromMatchId[cacheKey] != 'undefined') {
            return cacheMarketTypeFromMatchId[cacheKey];
        }
        const responseData = await axios.post(
            config.apiUrls.GetMatchMarketID,
            { MatchID: matchId },
            { headers: { "Content-Type": "application/json", apiKey: config_url.apiKey } }
        );
        if (responseData.status) {
            const datas = responseData.data.data
            for (const data of datas) {
                tmpCacheKey = "match_market_map_" + data.MatchID + '-' + data.MarketID
                cacheMarketTypeFromMatchId[tmpCacheKey] = data;
            }
            if (typeof cacheMarketTypeFromMatchId[cacheKey] != 'undefined') {
                return cacheMarketTypeFromMatchId[cacheKey];
            }
            return false;
        }
    } catch (e) {
        console.log("Error while fetching market type");
        console.log(e);
        return false;
    }
}

async function calculateResult() {
    console.log('start calculateResult')

    /* await axios.get(
        process.env.API_PNL_CALCULATION,
        { headers: { "Content-Type": "application/json" } },
    ).then(function (response) {
        console.log("Result declaration response : ", response.data);
    }).catch(({ response }) => {
        console.log("ERR : Got " + response.status + " in - " + response.config.url);
    })
    return; */

    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }

    let betData = await Bet.find({ wins_status: "", status: 1 });
    if (betData != null && betData.length > 0) {
        for (const bet of betData) {
            let result = {}
            const { matchid, operatorid, market_type } = customLib.cleanTheBody(bet)
            if (market_type == config.markettype.MATCHODDS) {
                result = await declareResult.calculateOddResult(bet)
            } else if (market_type == config.markettype.BACKLAY) {
                result = await declareResult.calculateBackLayfancyResult(bet)
            } else if (market_type == config.markettype.SPECIAL) {
                result = await declareResult.calculateSpecialfancyResult(bet)
            } else if (market_type == config.markettype.YESNO) {
                result = await declareResult.calculateYesnofancyResult(bet)
            }
            if (Object.keys(result).length > 0) {
                try {
                    if (result["wins_status"] != '') {
                        if (result["wins_status"] == "hold") {
                            await Bet.updateOne({ guid: bet.guid }, { wins_status: result["wins_status"] }, function (err, result) {
                            }).clone();
                        }
                        let data = await getTblAdminData(operatorid)
                        const { multiple_factor } = data
                        final_profit_loss = result["profit_loss"] * multiple_factor * -1
                        win_selection_id = (typeof result["win_selection_id"] != 'undefined') ? result["win_selection_id"] : ''
                        await Bet.updateOne({ guid: bet.guid }, {
                            wins_status: result["wins_status"],
                            profit_loss: result["profit_loss"], //this is for customer profit loss
                            final_profit_loss: final_profit_loss //this is for company profit loss
                        }, function (err, result) {
                            if (err) {
                                console.log('Error while updating result in mongodb');
                                console.log(err);
                            }
                        }).clone();
                        if (win_selection_id != '' && win_selection_id != 0) {
                            await insertMatchWinner(matchid, win_selection_id);
                        }
                    }
                } catch (e) {
                    console.log('Exception while calculateResult in mongodb');
                    console.log(e);
                }
            }
        }
    }
    await moveToHistory();
    console.log('end calculateResult')
}

async function insertMatchWinner(matchid, win_selection_id) {
    await mysql_accounting.getConnection(function (err, con) {
        if (err) {
            realeaseSQLCon(con);
            return false;
        }
        con.query(
            "INSERT IGNORE INTO `match_winners` (`match_id`, `winner_id`) VALUES (?,?)",
            [matchid, win_selection_id],
            async function (err, result, fields) {
                if (err) {
                    console.log("Error occured while updating win_selection_id-" + win_selection_id + ", in match id-" + matchid + ".");
                    console.log(err);
                    realeaseSQLCon(con);
                } else {
                    deleteCwinClossEntry(matchid)
                }
            })
    })
}

async function feedRadisResultData() {
    customLib.connectMongoDB();
    console.log("Declare result in redis");
    matchIds = await getMatchIdsOfLiveBets();
    if (matchIds) {
        ResultLiveUpComing(matchIds);

        matchIds.forEach(matchid => {
            ResultFancy(matchid);
        })
    }
};

async function ResultLiveUpComing(matchIds = '') {
    matchIds = matchIds.toString();
    if (matchIds != '') {
        fullApiPath = process.env.API_GetProbabilityPlayerScoreCompleted;
        let apiKey = process.env.API_KEY_GetProbabilityPlayerScoreCompleted;
        const response = await makeApiCall(fullApiPath, apiKey, matchIds);
        if (typeof response === 'object' && response !== null && !Array.isArray(response)) {
            if (response.data.length > 0) {
                await redisClient.lPush('GetProbabilityPlayerScoreCompleted', JSON.stringify(response));
                let listItemCount = await redisClient.lLen('GetProbabilityPlayerScoreCompleted');
                if (listItemCount > 10) {
                    await redisClient.rPop('GetProbabilityPlayerScoreCompleted');
                }
            }
        }
    }
}

async function ResultFancy(match_id) {
    ResultYesNofancy(match_id);
    ResultBackLayfancy(match_id);
    ResultSpecialfancy(match_id);
}

async function ResultYesNofancy(match_id) {
    fullApiPath = process.env.API_GetYesNoFancyResult;
    let apiKey = process.env.API_KEY_GetYesNoFancyResult;
    const response = await makeApiCall(fullApiPath, apiKey, match_id);
    if (typeof response === 'object' && response !== null && !Array.isArray(response)) {
        if (response.data.length > 0) {
            let currentTime = moment().format('YYYY-MM-DD hh:mm:ss');
            await redisClient.lPush(`YesNofancyResultForMatchId:${match_id}`, JSON.stringify(response));
            await redisClient.del(`LastUpdatedTimeResult_YesNofancyForMatchId:${match_id}`);
            await redisClient.sAdd(`LastUpdatedTimeResult_YesNofancyForMatchId:${match_id}`, currentTime);
            let listItemCount = await redisClient.lLen(`YesNofancyResultForMatchId:${match_id}`);
            if (listItemCount > 1) {
                await redisClient.rPop(`YesNofancyResultForMatchId:${match_id}`);
            }
        }
    }
}

async function ResultBackLayfancy(match_id) {
    fullApiPath = process.env.API_GetBackLayfancyResult;
    let apiKey = process.env.API_KEY_GetBackLayfancyResult;
    const response = await makeApiCall(fullApiPath, apiKey, match_id);
    if (typeof response === 'object' && response !== null && !Array.isArray(response)) {
        if (response.data.length > 0) {
            let currentTime = moment().format('YYYY-MM-DD hh:mm:ss');
            await redisClient.lPush(`BackLayfancyResultForMatchId:${match_id}`, JSON.stringify(response));
            await redisClient.del(`LastUpdatedTimeResult_BackLayfancyForMatchId:${match_id}`);
            await redisClient.sAdd(`LastUpdatedTimeResult_BackLayfancyForMatchId:${match_id}`, currentTime);
            let listItemCount = await redisClient.lLen(`BackLayfancyResultForMatchId:${match_id}`);
            if (listItemCount > 1) {
                await redisClient.rPop(`BackLayfancyResultForMatchId:${match_id}`);
            }
        }
    }
}

async function ResultSpecialfancy(match_id) {
    fullApiPath = process.env.API_GetSpecialfancyResult;
    let apiKey = process.env.API_KEY_GetSpecialfancyResult;
    const response = await makeApiCall(fullApiPath, apiKey, match_id);
    if (typeof response === 'object' && response !== null && !Array.isArray(response)) {
        if (response.data.length > 0) {
            let currentTime = moment().format('YYYY-MM-DD hh:mm:ss');
            await redisClient.lPush(`SpecialfancyResultForMatchId:${match_id}`, JSON.stringify(response));
            await redisClient.del(`LastUpdatedTimeResult_SpecialfancyForMatchId:${match_id}`);
            await redisClient.sAdd(`LastUpdatedTimeResult_SpecialfancyForMatchId:${match_id}`, currentTime);
            let listItemCount = await redisClient.lLen(`SpecialfancyResultForMatchId:${match_id}`);
            if (listItemCount > 1) {
                await redisClient.rPop(`SpecialfancyResultForMatchId:${match_id}`);
            }
        }
    }
}

async function makeApiCall(apiPath, apiKey, matchID = "") {
    const headers = {
        'Content-Type': 'application/json',
        'APIKey': apiKey,
    }
    this.data = { "MatchID": matchID };
    const response = await axios.post(apiPath, (this.data), {
        headers: headers
    });

    if (response.data && response.data.status) {
        return response.data;
    }
}

async function deleteCwinClossEntry(matchID = '') {
    if (matchID == '') {
        return false;
    }
    var cwin_closs = null;
    if (mongoose.models.cwin_closs) {
        cwin_closs = mongoose.model('cwin_closs');
    } else {
        cwin_closs = mongoose.model('cwin_closs', model_winLoss);
    }

    cwin_closs.deleteMany({ matchid: matchID }).then(function () {
        console.log("Data deleted");
    }).catch(function (error) {
        console.log(error);
    });
}

function initSocket() {
    io.on('connection', socket => {
        socket.on('connected', async data => {
            console.log("socket connected");
            const found = Watchers.findIndex(e => e.uid == data.uid)
            if (found > -1) {
                Watchers[found]['sid'] = socket.id
            } else {
                Watchers.push({ uid: data.uid, sid: socket.id })
            }
            socket.join(socket.id)
            await broadcastPNLUpdate() //send initial data of PNL
            await broadcastPNLDetailsUpdate() //send initial data of PNL
        })

        socket.on('disconnect', data => {
            try {
                const index = Watchers.findIndex(e => e.sid == socket.id)
                if (index > -1) {
                    Watchers.splice(index, 1);
                }
            } catch (e) {
                console.log('Err while disconnect');
                console.log(e);
            }
            console.log('disconnect', socket.id);
        })
    })
}

async function broadcastPNLDetailsUpdate() {
    cwin_closs_model = null;
    if (mongoose.models.cwin_closs) {
        cwin_closs_model = mongoose.model('cwin_closs');
    } else {
        cwin_closs_model = mongoose.model('cwin_closs', model_winLoss);
    }
    yesnoPnlModel = null
    if (mongoose.models.yesno) {
        yesnoPnlModel = mongoose.model('yesNOPNL');
    } else {
        yesnoPnlModel = mongoose.model('yesNOPNL', model_yesno);
    }
    winlossData = await cwin_closs_model.find();
    let pnlData = {}
    if (winlossData != null) {
        try {
            winlossData.forEach(element => {
                if (typeof pnlData[element.matchid] == 'undefined') {
                    pnlData[element.matchid] = {
                        'odds': {},
                        'special_fancy': {},
                        'backlay_fancy': {},
                        'yesno_fancy': {},
                    }
                }
                if (element.markettype == config.markettype.MATCHODDS) {
                    if (typeof pnlData[element.matchid]['odds'][element.selectionid] == 'undefined') {
                        pnlData[element.matchid]['odds'][element.selectionid] = element.pnl
                    }
                }
                if (element.markettype == config.markettype.BACKLAY) {
                    if (typeof pnlData[element.matchid]['backlay_fancy'][element.marketid] == 'undefined') {
                        pnlData[element.matchid]['backlay_fancy'][element.marketid] = {}
                    }
                    if (typeof pnlData[element.matchid]['backlay_fancy'][element.marketid][element.selectionid] == 'undefined') {
                        pnlData[element.matchid]['backlay_fancy'][element.marketid][element.selectionid] = element.pnl
                    }
                }
                if (element.markettype == config.markettype.SPECIAL) {
                    if (typeof pnlData[element.matchid]['special_fancy'][element.mfancyid] == 'undefined') {
                        pnlData[element.matchid]['special_fancy'][element.mfancyid] = {}
                    }
                    if (typeof pnlData[element.matchid]['special_fancy'][element.mfancyid][element.selectionid] == 'undefined') {
                        pnlData[element.matchid]['special_fancy'][element.mfancyid][element.selectionid] = element.pnl
                    }
                }
                if (element.markettype == config.markettype.YESNO) {

                }
            })
        } catch (e) {
            console.log(e);
        }
    }
    winlossYesNoData = await yesnoPnlModel.find();
    if(winlossYesNoData != null){
        winlossYesNoData.forEach(element => {
            if (typeof pnlData[element.matchid] == 'undefined') {
                pnlData[element.matchid] = {
                    'odds': {},
                    'special_fancy': {},
                    'backlay_fancy': {},
                    'yesno_fancy': {},
                }
            }
            if (typeof pnlData[element.matchid]['yesno_fancy'][element.mfancyid] == 'undefined') {
                pnlData[element.matchid]['yesno_fancy'][element.mfancyid] = {}
            }
            if (typeof pnlData[element.matchid]['yesno_fancy'][element.mfancyid][element.beton] == 'undefined') {
                pnlData[element.matchid]['yesno_fancy'][element.mfancyid][element.beton] = element.pnl
            }
        })
    }

    //console.log("detail pnl data")
    Watchers.map(e => {
        io.sockets.in(e.sid).emit('pnl_details_update', pnlData)
    })
}

async function broadcastPNLUpdate() {
    pnlData = []
    console.log('broadcastPNLUpdate');
    cwin_closs_model = null;
    yesno_model = null
    if (mongoose.models.cwin_closs) {
        cwin_closs_model = mongoose.model('cwin_closs');
    } else {
        cwin_closs_model = mongoose.model('cwin_closs', model_winLoss);
    }

    if (mongoose.models.yesno) {
        yesno_model = mongoose.model('yesNOPNL');
    } else {
        yesno_model = mongoose.model('yesNOPNL', model_yesno);
    }

    yesNoData = await yesno_model.find();
    yesNoPnlData = {}
    if (yesNoData != null) {
        try {
            await yesNoData.forEach(element => {
                tmpCacheKey = "yesno_pnl_data_" + element.matchid
                if (typeof yesNoPnlData[tmpCacheKey] == 'undefined') {
                    yesNoPnlData[tmpCacheKey] = {
                        'match_id': element.matchid,
                        'yesno_fancy_stack': 0,
                    };
                }
                cacheData = yesNoPnlData[tmpCacheKey];
                if (cacheData.yesno_fancy_stack > element.pnl) {
                    cacheData.yesno_fancy_stack = parseFloat(element.pnl.toFixed(2))
                    yesNoPnlData[tmpCacheKey] = cacheData
                }
            });
        } catch (e) {
            console.log(e);
        }
    }

    matchOddsStack = 0;
    specialFancyStack = 0;
    yesNoFancyStack = 0;
    backlayFancyStack = 0;

    winlossData = await cwin_closs_model.find();
    pnlData = {}
    if (winlossData != null) {
        try {
            winlossData.forEach(element => {
                tmpCacheKey = "pnl_data_" + element.matchid
                tmpYesNoCacheKey = "yesno_pnl_data_" + element.matchid
                if (typeof pnlData[tmpCacheKey] == 'undefined') {
                    pnlData[tmpCacheKey] = {
                        'match_id': element.matchid,
                        'odds': {},
                        'odd_stack': 0,
                        'special_fancy_stack': 0,
                        'yesno_fancy_stack': 0,
                        'backlay_fancy_stack': 0,
                    };
                }
                cacheData = pnlData[tmpCacheKey]
                tmpYesNoData = yesNoPnlData[tmpYesNoCacheKey];
                if (tmpYesNoData) {
                    cacheData.yesno_fancy_stack = tmpYesNoData.yesno_fancy_stack
                }
                if (element.markettype == config.markettype.MATCHODDS) {
                    cacheData['odds'][element.selectionidname] = element.pnl.toFixed(2)
                    if ((cacheData.odd_stack * 1) > (element.pnl * 1)) {
                        cacheData.odd_stack = parseFloat(element.pnl.toFixed(2))
                    }
                    //cacheData.odd_stack = parseFloat(cacheData.odd_stack.toFixed(2)) + parseFloat(element.pnl.toFixed(2))
                } else if (element.markettype == config.markettype.SPECIAL) {
                    if (cacheData.special_fancy_stack > element.pnl) {
                        cacheData.special_fancy_stack = parseFloat(element.pnl.toFixed(2))
                    }
                    //cacheData.special_fancy_stack = parseFloat(cacheData.special_fancy_stack.toFixed(2)) + parseFloat(element.pnl.toFixed(2))
                } else if (element.markettype == config.markettype.BACKLAY) {
                    if (cacheData.backlay_fancy_stack > element.pnl) {
                        cacheData.backlay_fancy_stack = parseFloat(element.pnl.toFixed(2))
                    }
                    //cacheData.backlay_fancy_stack = parseFloat(cacheData.backlay_fancy_stack.toFixed(2)) + parseFloat(element.pnl.toFixed(2))
                }
                pnlData[tmpCacheKey] = cacheData
            })
        } catch (e) {
            console.log(e);
        }
    }
    for (let matchId in matchOddCache) {
        if (matchOddCache[matchId].Status == 'Live') {
            tmpCacheKey = "pnl_data_" + matchId
            if (typeof pnlData[tmpCacheKey] == 'undefined') {
                pnlData[tmpCacheKey] = {
                    'match_id': matchId,
                    'odds': {},
                    'odd_stack': 0,
                    'special_fancy_stack': 0,
                    'yesno_fancy_stack': 0,
                    'backlay_fancy_stack': 0,
                }
            }
        }
    }

    console.log('sending pnl data');
    Watchers.map(e => {
        io.sockets.in(e.sid).emit('pnl_update', pnlData)
    })
}

async function getMatchIdsOfLiveBets() {
    liveMatchIds = []

    var betModel = null;
    if (mongoose.models.bets) {
        betModel = mongoose.model('bets');
    } else {
        betModel = mongoose.model('bets', model_bet);
    }

    await betModel.aggregate(
        [
            {
                $group:
                {
                    _id: { match: "$matchid" }
                }
            }
        ],
        function (err, docs) {
            if (err) {
                console.log("ERR : in getting live match ids")
                return;
            }

            docs.forEach(element => {
                liveMatchIds.push(element['_id']['match']);
            })
        }
    );
    return liveMatchIds;
}

function initGameServerSocket() {
    try {
        const sio = require("socket.io-client");

        let socketReceive = sio(process.env.NODEJS_SOCKET_URL, {
            transports: ['websocket'],
            secure: true,
            auth: {
                api_key: process.env.SOCKET_TOKEN,
                type: 'verify_server'
            }
        })

        socketReceive.on('connect', () => {
            console.log('Game Play connected - ' + socketReceive.id)
            const uid = Math.floor((Math.random() * 1000000) + 1)
            socketReceive.emit('connected', { id: socketReceive.id, uid })
        });
        socketReceive.on('odds_update', async (data) => {
            matchOddCache = data
        })
        socketReceive.on("connect_error", (err) => {
            //console.log(`connect_error due to ${err.message} hitesh`);
            console.log(err);
        });
        socketReceive.on('fancy_update', async (data) => {
            console.log("fancy_update")
            matchId = data.match_id;
            if (data.type == "backlay") {
                matchBacklayCache[matchId] = data.data;
            } else if (data.type == "special") {
                matchSpecialCache[matchId] = data.data;
            } else if (data.type == "yesno") {
                matchYesnoCache[matchId] = data.data;
            }
        })

        socketReceive.on('disconnect', (data) => {
            console.log('Game play disconnected - ', socketReceive.id);
        })

        socketReceive.on("connect_error", (err) => {
            console.log(`connect_error due to ${err.message}`);
        });
    } catch (e) {
        console.log("Exception in initIO()");
        console.log(e);
    }
}

async function moveUnmatchedBets() {
    await customLib.connectMongoDB();
    let Bet = null
    if (mongoose.models.bets) {
        Bet = mongoose.model('bets');
    } else {
        Bet = mongoose.model('bets', model_bet);
    }

    let betData = await Bet.find({ status: { $nin: ['1'] } });
    for (const bet of betData) {
        const {
            matchid, operatorid, sportsid, userid, mfancyid, selectionid, marketid, beton, odds,
            backlayyesno, exposure_pnl, stake, profit_loss, wins_status, currency, final_stack,
            final_exposure_pnl, final_profit_loss, guid, status, datetimestamp, fancy_family,
            fancy_name, fancy_reference, ip_address, market_type
        } = customLib.cleanTheBody(bet)

        let moveRecord = await new Promise((resolve, reject) => {
            mysql_accounting.getConnection(function (err, con) {
                if (err) {
                    realeaseSQLCon(con);
                    return reject(false);
                }
                con.query(
                    "INSERT IGNORE INTO `bet`(`operatorid`, `matchid`, `sportsid`, `userid`, `mfancyid`, `market_type`,`fancy_family`, `fancy_name`, `fancy_reference`, `selectionid`, `marketid`, `beton`, `odds`, `backlayyesno`, `stake`, `profit_loss`, `exposure_pnl`, `status`, `wins_status`, `datetimestamp`, `currency`, `final_stack`, `final_profit_loss`, `final_exposure_pnl`, `guid`, `ip_address`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [
                        operatorid, matchid, sportsid, userid, mfancyid, market_type, fancy_family, fancy_name, fancy_reference,
                        selectionid, marketid, beton, odds, backlayyesno, stake, profit_loss,
                        exposure_pnl, status, wins_status, datetimestamp, currency, final_stack,
                        final_profit_loss, final_exposure_pnl, guid, ip_address
                    ],
                    async function (err, result, fields) {
                        realeaseSQLCon(con);
                        console.log("inserted in DB : ", guid)
                        if (err) {
                            console.log("Error in mysql query to update user pnl");
                            console.log(err);
                            return reject(false);
                        }
                        let movedBetsData = {
                            operatorid: operatorid,
                            sportsid: sportsid,
                            matchid: matchid,
                            marketid: marketid,
                            userid: userid,
                            market_type: market_type,
                            mfancyid: mfancyid,
                            fancy_family: fancy_family,
                            fancy_name: fancy_name,
                            guid: guid,
                            datetimestamp: datetimestamp
                        }
                        await createBetBackup(movedBetsData)
                        /* console.log("deleting start for  : ", guid)
                        deleteData = await Bet.deleteOne({ guid: guid }).then(function () {
                            customLib.log(guid, "deleted_from_mongo_unmatched")
                        }).catch(function (error) {
                            customLib.log(guid, "unable_to_delete_from_mongo_unmached")
                        });
                        console.log("deleting end for  : ", guid) */
                        return resolve(result);
                    }
                );
            });
        });
    }
}

function createBetBackup(movedBetsData) {
    let movedBetsModel = null
    if (mongoose.models.moved_bets) {
        movedBetsModel = mongoose.model('moved_bets');
    } else {
        movedBetsModel = mongoose.model('moved_bets', model_moved_bets);
    }

    var betModel = new movedBetsModel(movedBetsData);
    return betModel.save().then(async function (result) {
        customLib.log(result.guid, "saved_to_bet_backup")
        return true
    }).catch(function (err) {
        customLib.log(movedBetsData.guid, "unable_to_save_in_backup")
        console.log("Err while saving backup bet", err)
        return false
    });
    return false
}

async function generateCacheData() {
    console.log("generateCacheData");
    await customLib.connectMongoDB();

    cacheTblAdminDataGenerate();
    await stackDataCache();
    //feedRadisResultData();
    //calculateResult();

    setInterval(function () {
        cacheTblAdminDataGenerate()
    }, 10 * 60 * 1000);

    setInterval(function () {
        stackDataCache()
    }, 10 * 60 * 1000);

    setInterval(async function () {
        await generatePNL()
        //await winloss.updateWinLoss()
    }, 2 * 1000); //Run on every 2 Seconds

    setInterval(async function () {
        await calculateResult()
    }, 1 * 60 * 1000); //Run on every 1 Minute

    setInterval(async function () {
        await moveUnmatchedBets()
    }, 5 * 60 * 1000); //Run on every 5 Minute

    /* setInterval(function () {
        feedRadisResultData()
    }, 5 * 10 * 1000); //Run on every 1 Minute */
}

generateCacheData();
initGameServerSocket();

//CLUSTERING
/* if (cluster.isMaster) {
    console.log(`\nTotal number of CPUs is ${totalCPUs}`);
    console.log(`Master is running at, pid : ${process.pid}\n`);
    for (let i = 0; i < totalCPUs; i++) {
        cluster.fork()
    }
    cluster.on('exit', (worker, code, signal) => {
        console.log(`worker ${worker.process.pid} died`)
        cluster.fork()
    })
} else {
    http.listen(config.port, () => console.log("-------------------------------\n|    Bet server is running on port : " + config.port + ", pid : " + process.pid + "   |\n-------------------------------"));
} */
//CLUSTERING
