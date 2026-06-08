/*
 *
 */

use std::{
  io::{self, Read, Write},
  net::{TcpListener, TcpStream},
  sync::{Arc, Mutex},
  thread,
};

type Clients = Arc<Mutex<Vec<TcpStream>>>;

/*
 *
 */
fn broadcast(message: &[u8], clients: &Clients) -> io::Result<()> {
  let mut clients = clients.lock().unwrap();

  clients.retain_mut(|client| {
    if let Err(e) = client.write_all(message) {
      eprintln!("Removing dead client: {e}");
      return false; // remove broken connection
    }
    true
  });

  Ok(())
}

/*
 *
 */
fn handle_client(mut stream: TcpStream, clients: Clients) -> io::Result<()> {
  // Add client to shared list
  {
    let mut locked = clients.lock().unwrap();
    locked.push(stream.try_clone()?);
  }

  let mut buffer = [0u8; 1024];

  loop {
    match stream.read(&mut buffer) {
      Ok(0) => {
        println!("Client disconnected");
        break;
      }
      Ok(n) => {
        println!("Received: {}", String::from_utf8_lossy(&buffer[..n]));
        // stream.write_all(&buffer[..n])?;
        broadcast(&buffer[..n], &clients)?;
      }
      Err(e) => {
        eprintln!("Read error: {e}");
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
  println!("Chat server running on 127.0.0.1:7878");

  let clients: Clients = Arc::new(Mutex::new(Vec::new()));

  for stream in listener.incoming() {
    match stream {
      Ok(stream) => {
        let peer_addr = stream.peer_addr()?;
        println!("Client connected: {peer_addr}");

        let clients_clone = Arc::clone(&clients);

        thread::spawn(move || {
          if let Err(e) = handle_client(stream, clients_clone) {
            eprintln!("Client {peer_addr} error: {e}");
          } else {
            println!("Client {peer_addr} disconnected");
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
