const express = require('express')
const colors = require('colors')
const dotenv = require('dotenv').config()
const cors = require('cors')
//const { errorHandler } = require('./middleware/errorMiddleware')
const connectDB = require('./config/db')
//const bodyParser = require('body-parser');
const port = process.env.PORT || 4001

connectDB()

const app = express();

app.use(express.json());
app.use(cors())
app.use(express.urlencoded({extended : false}))

app.use("/getPages" , require('./routes/getDocument'))

app.listen(port , () => console.log(`server started on port ${port}`));
