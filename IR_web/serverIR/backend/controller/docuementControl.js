const asynchandler = require('express-async-handler')
//const docObject = require('../model/document')
//const sample_collection = require('../model/document')
//const {agenda} = require('../model/document')
const { removeStopwords } = require('stopword')
const stemmer = require("porter-stemmer-english")
var HashMap = require('hashmap');
const router = require('../routes/getDocument')
//const {docLink} = require('../model/documentLinks');
//const {} = require('../model/documentLinks')
//const { countDocuments } = require('../model/document');
const {docObject , docLink } = require('../model/combinedLink')


const getLinks = asynchandler(async (arrayObj) => {
    var q = []
    arrayObj.forEach(ele => {
        q.push(ele.document.split('.')[0])
    })
    const docs = await docLink.find({doc: {$in: q}})
    var docMap = new HashMap();
    docs.forEach( t => {
        docMap.set(t.doc , {"link" : t.link , "text" : t.text});
    } )
    arrayObj.forEach(ele => {
        ele.document = docMap.get(ele.document.split('.')[0])
    } , arrayObj)
    finalArrayObject = []
    arrayObj.forEach(ele => {
        finalArrayObject.push(ele.document)
    })

    return finalArrayObject
})

const getDocuments = asynchandler(async (req,res) => {
    //console.log(req.body.text)
    var data = stemQuery(req.body.text)
   // console.log("data " + data)
    var docpages = await docObject.find({word: {$in: data}})
    var finalArray  = filterDocuments(docpages)
    //console.log("get done")
    returnObj = await getLinks(finalArray)
    res.status(200).json(returnObj);
})

function stemQuery(data){ 
    newData = removeStopwords(data.split(' '))
    newData = newData.join(' ');
    var natural = require('natural');   
    var tokenizer = new natural.WordTokenizer();
    var tokens = tokenizer.tokenize(newData);
    var finData = []
    tokens.forEach(token => { 
        var t = stemmer(token)
        if(finData.indexOf(t) < 0) {
            finData.push(stemmer(token))
        }
    });    
    //console.log("stemming completed")
    return finData
} 

// fin Arr for score -> documents
// need to divide with no of queries it appeared
function filterDocuments(pageObj) {
    //console.log("filtering documents " +pageObj)
    var mp = new HashMap()
    var mpset = new HashMap()
    console.log("filtering documents")
    pageObj.forEach((page) => {
        //console.log(page)
        page.doc.forEach((dobj) => {
            if(mp.has(dobj.document)) {
                mp.set(dobj.document , (mp.get(dobj.document)+ Number(dobj.score))/(mpset.get(dobj.document)+1))
                //mp.set(dobj.document , mp.get(dobj.document)+ Number(dobj.score))
                mpset.set(dobj.document , mpset.get(dobj.document)+1)
            }else{
                mpset.set(dobj.document , 1)
                mp.set(dobj.document , Number(dobj.score))
            }
        })
    })
    console.log("filtering completed ")
    var finArr = []
    mp.forEach(function(score, document) {
        finArr.push({score , document })
    });
    mpset.forEach(function(score, document) {
        console.log(score + " " + document)
    });
    finArr.sort((a, b) => a.score < b.score ? 1 : -1);
    
    finArr.forEach(t => {
        console.log(t.score + " " + t.document)
    })
    
    return finArr
}

const postDocuments = asynchandler(async (req,res) => {
    console.log(req.body)
    const insertWord = await docObject.create(
        {
            word : req.body.word,
            docs : req.body.docs
        }
    ) 
    res.status(200).send(insertWord)
})

module.exports = {
    getDocuments, 
    postDocuments,
    getLinks,
};