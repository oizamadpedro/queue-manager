from producer import send_to_queue
from fastapi import FastAPI, Request
from datetime import datetime
from db import SQLiteService
import uuid
import json

app = FastAPI()

class Queue():
    """
        Queue Status:
        - IN_QUEUE
        - IN_PROCESSING
        - COMPLETED
        - ERROR
    """

    def start(self, body):
        current_timestamp = datetime.now().isoformat()
        combined = current_timestamp + str(body)

        id = str(uuid.uuid5(uuid.NAMESPACE_DNS, combined))
        status = 'IN_QUEUE'
        
        q = "INSERT INTO process(id, status) values (?, ?)"
        values = (id, status)
        
        process_row = SQLiteService.commit(q, values)

        return {
            "process_id": id,
            "status": status,
            "process_row": process_row
        }
    
    def processing(self, id):
        status = 'IN_PROCESSING'
        q = "UPDATE process SET status=? where id=?;"
        values = (status, id)

        process_row = SQLiteService.commit(q, values)

        return {
            "process_id": id,
            "status": status,
            "process_row": process_row
        }
    
    def completed(self, id):
        status = 'COMPLETED'
        q = "UPDATE process SET status=? where id=?;"
        values = (status, id)

        process_row = SQLiteService.commit(q, values)

        return {
            "process_id": id,
            "status": status,
            "process_row": process_row
        }

@app.post('/queue')
async def queue(request: Request):
    body = await request.body()
    received_response = json.loads(body.decode("utf-8"))

    start_queue = Queue().start(received_response)

    received_response.update({
        "process_id": start_queue.get('process_id')
    })
    
    send_to_queue(str(received_response))
    
    return {
        "message": "send to queue",
        "response": str(received_response)
    }