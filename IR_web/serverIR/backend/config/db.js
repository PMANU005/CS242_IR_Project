const mongoose = require('mongoose')

const connectDB = async() => {
    try{
        //const conn = await mongoose.connect("mongodb+srv://akumm002:Avinash890@cluster0.brs1r.mongodb.net/IR?retryWrites=true&w=majority");
        const conn = await mongoose.connect("mongodb+srv://dbuser:hellomongo@cluster0.1rvee.mongodb.net/sample_db?retryWrites=true&w=majority");
       // mongoose.model()
        console.log(`Mongo Connected : ${conn.connection.host} `);
    } catch (error) {
        console.log(error);
        process.exit(1);
    }
}

module.exports = connectDB