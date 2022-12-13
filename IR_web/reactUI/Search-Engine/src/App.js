import "./styles.css";
import CustomCard from "./customCard";
import { response } from "./input";
import { Button, Form ,InputGroup, FormControl, DropdownButton, Dropdown} from "react-bootstrap";
import { useState } from "react";
import { options } from "./options";
export default function App() {
  var [responseData, setResponseData] = useState([]);
  var [query,setQuery] = useState('');
  const modesList = ["Hadoop","Leucene"];
  var [mode,setMode] = useState(modesList[0]); // default is taken as Hadoop

  var handleSearch = (query)=> {
      fetch("http://localhost:4001/getPages",{
        method: 'POST',
        body: new URLSearchParams({
            'text': query
        })
    }).then((response) => response.json())
        .then((json) => setResponseData(json));
  }

  const mystyle = {
      display:"flex",
      flexDirection:"column",
      displayContent:"center",
      alignItems:"center"
    };

  return (
    <div>
      <div style={mystyle}>
        <InputGroup size = "lg" className="w-75 m-4 p-4">
          <FormControl
            placeholder="Enter Query Here..."
            onChange={(e) => {setQuery(e.target.value);setResponseData([])}}
          />
          <Button
            variant="outline-secondary"
            id="button-addon2"
            onClick={() => {
              console.log(query), handleSearch(query);
            }}
          >
            Search
          </Button>
          <DropdownButton
            variant="outline-secondary"
            title={mode}
            id="input-group-dropdown-1"
          >
          {
            modesList.map((mode,id) => (
              <Dropdown.Item href="#" key={id}>
              <Button
                variant="outline-none"
                value={mode}
                onClick={(e) => {
                  setMode(e.target.value)
                  setResponseData([])
                }}
              >
                {mode}
              </Button>
            </Dropdown.Item>))
          }
          </DropdownButton>
        </InputGroup>
      </div>
      <div>
        <div className="text-center">
        {responseData.length ? ("Total Search Results Found : "+ responseData.length) : null}
        </div>
        {responseData.map((r) => (
          <CustomCard Link={r.link} Text={r.text}></CustomCard>
        ))}
      </div>
    </div>
  )
}
