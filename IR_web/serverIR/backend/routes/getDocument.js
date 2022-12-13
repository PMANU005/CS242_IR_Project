const express = require('express')
const router = express.Router()
const {getDocuments , postDocuments , getLinks} = require("../controller/docuementControl")
const docObject = require('../model/document')

router.post( '/' , getDocuments );
//router.get( '/getLink' , getLinks );
router.post('/' , postDocuments);

module.exports = router
