/*
 *
 */

use std::{
  io::{self, Read, Write, BufReader, BufRead},
  net::{TcpListener, TcpStream},
  sync::{Arc, Mutex},
  thread,
};

use serde::{
  Deserialize,
  de::DeserializeOwned,
  Serialize,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;

#[derive(Deserialize, Debug)]
struct Request {
  action:  String,
  name:    Option<String>,
  message: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
struct Response {
  action:  String,
  user:    Option<String>,
  message: Option<String>,
}

/*
 *
 */
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct Client {
  id:   u64,
  name: String,
  room: Option<String>,
  tx:   Sender<Response>,
}

type Clients = Arc<Mutex<Vec<Client>>>;

fn start_writer(mut stream: TcpStream, rx: std::sync::mpsc::Receiver<Response>) {
  thread::spawn(move || {
    for msg in rx {
      if let Err(e) = send(&mut stream, &msg) {
        eprintln!("send error: {e}");
        break;
      }
    }
  });
}

/*
 *
 */
fn broadcast(client_id: u64, clients: &Clients, response: Response) -> io::Result<()> {
  let clients = clients.lock().unwrap();

  for c in clients.iter() {
    if c.id != client_id {
      let _ = c.tx.send(response.clone());
    }
  }

  Ok(())
}

/*
 *
 */
fn recv<T: DeserializeOwned> (reader: &mut BufReader<TcpStream>) -> io::Result<T> {
  let mut line = String::new();

  let bytes = reader.read_line(&mut line)?;

  if bytes == 0 {
    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "client disconnected"));
  }

  let value = serde_json::from_str(line.trim_end())
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

  Ok(value)
}

/*
 *
 */
fn send<T: Serialize> (stream: &mut TcpStream, value: &T) -> io::Result<()> {
  let mut json = serde_json::to_string(value)
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

  json.push('\n');

  stream.write_all(json.as_bytes())?;
  stream.flush()?;

  Ok(())
}

/*
 *
 */
fn handle_client(stream: TcpStream, clients: Clients) -> io::Result<()> {
  let mut reader = BufReader::new(stream.try_clone()?);

  let request: Request = recv(&mut reader)?;

  if request.action != "connect" {
    return Ok(());
  }

  let client_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

  let name = request.name.unwrap_or_else(|| "anonymous".to_string());

  // ===== create channel =====
  let (tx, rx) = std::sync::mpsc::channel::<Response>();

  // ===== spawn writer thread =====
  start_writer(stream.try_clone()?, rx);

  let client = Client {
    id: client_id,
    name: name.clone(),
    room: None,
    tx,
  };

  {
    let mut locked = clients.lock().unwrap();
    locked.push(client);
  }

  let sender_name = {
    let locked = clients.lock().unwrap();
    locked
      .iter()
      .find(|c| c.id == client_id)
      .map(|c| c.name.clone())
      .unwrap_or_else(|| "unknown".to_string())
  };

  {
    let msg: String = format!("{} has connected", sender_name.clone());

    println!("{}", msg);

    broadcast(client_id, &clients, Response {
      action:  "info".to_string(),
      user:    None,
      message: Some(msg.to_string()),
    });
  }

  // ===== main read loop =====
  loop {
    match recv::<Request>(&mut reader) {
      Ok(request) => {
        println!("{:?}", request);

        match request.action.as_str() {
          "message" => {
            if let Some(msg) = request.message {
              broadcast(client_id, &clients, Response {
                action:  "message".to_string(),
                user:    Some(sender_name.clone()),
                message: Some(msg.to_string()),
              });
            }
          },
          _ => println!("Unknown action: {}", request.action),
        }
      }
      Err(e) => {
        eprintln!("Receive error: {}", e);

        if e.kind() == io::ErrorKind::UnexpectedEof {
          let msg: String = format!("{} has disconnected", sender_name.clone());

          println!("{}", msg);

          broadcast(client_id, &clients, Response {
            action:  "info".to_string(),
            user:    None,
            message: Some(msg.to_string()),
          });
          break;
        }

        return Err(e);
      }
    }
  }
  Ok(())
}

/*
 *
 */
fn main() -> io::Result<()> {
  let listener = TcpListener::bind("127.0.0.1:7878")?;
  println!("Server running on 127.0.0.1:7878");

  let clients: Clients = Arc::new(Mutex::new(Vec::new()));

  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        let clients_clone = Arc::clone(&clients);

        thread::spawn(move || {
          if let Err(e) = handle_client(stream, clients_clone) {
            eprintln!("Client error: {e}");
          } else {
          }
        });
      }
      Err(e) => eprintln!("Accept error: {e}"),
    }
  }

  Ok(())
}

/*
fn login_user(stream: &mut TcpStream) -> String {
  println!("Logging in");
}

fn logout_user(stream: &mut TcpStream) -> String {
  println!("Logging out");
}

fn create_user(stream: &mut TcpStream) -> String {
  println!("Creating user");
}

fn delete_user(stream: &mut TcpStream) -> String {
  println!("Deleting user");
}

fn create_room(stream: &mut TcpStream) -> String {
  println!("Creating room");
}

fn delete_room(stream: &mut TcpStream) -> String {
  println!("Deleting room");
}

fn join_room(stream: &mut TcpStream) -> String {
  println!("Joining room");
}

fn leave_room(stream: &mut TcpStream) -> String {
  println!("Leaving room");
}
*/
