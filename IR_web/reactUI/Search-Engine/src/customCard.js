import React from "react";
import { Card } from "react-bootstrap";

function CustomCard(props) {
  return (
    <div className="card w-80 m-4">
      <h5 className="card-header"><small className="text-muted">{props.Link.split("/").pop()}</small></h5>
      <div className="card-body">
        <h5 className="card-title"><a href={props.Link} target="_blank">{props.Link}</a></h5>
        <p className="card-text">{props.Text}</p>
      </div>
    </div>
  );
}

export default CustomCard;
