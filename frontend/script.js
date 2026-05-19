const API = "http://localhost:3000";

// Navigation
function showSection(event, name) {
  document
    .querySelectorAll(".section")
    .forEach((s) => s.classList.remove("active"));
  document
    .querySelectorAll(".sidebar .nav-link")
    .forEach((l) => l.classList.remove("active"));
  document.getElementById("sec-" + name).classList.add("active");
  event.currentTarget.classList.add("active");
}

// Helpers
function pretty(obj) {
  return JSON.stringify(obj, null, 2);
}
function setRes(id, data) {
  document.getElementById(id).textContent =
    typeof data === "string" ? data : pretty(data);
}
function spin(id, on) {
  document
    .getElementById(id)
    .parentElement.classList[on ? "add" : "remove"]("loading");
}

async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(API + path, opts);
  return r.json();
}

// USERS
async function createUser() {
  const name = document.getElementById("u-name").value.trim();
  const email = document.getElementById("u-email").value.trim();
  if (!name || !email) {
    setRes("res-cu", "Fill fields");
    return;
  }
  spin("spin-cu", true);
  try {
    setRes("res-cu", await api("POST", "/users", { name, email }));
  } catch (e) {
    setRes("res-cu", "Error " + e.message);
  }
  spin("spin-cu", false);
}

async function getUser() {
  const id = document.getElementById("u-id").value.trim();
  if (!id) {
    setRes("res-gu", "Enter ID");
    return;
  }
  spin("spin-gu", true);
  try {
    setRes("res-gu", await api("GET", "/users/" + id));
  } catch (e) {
    setRes("res-gu", "Error " + e.message);
  }
  spin("spin-gu", false);
}

// PRODUCTS
async function listProducts() {
  const rows = await api("GET", "/products");
  const tbody = document.getElementById("tbody-products");
  tbody.innerHTML = rows
    .map(
      (p) => `
    <tr>
      <td>${p.name}</td>
      <td>${p.category}</td>
      <td>${p.price}</td>
      <td>${p.stock}</td>
      <td>${p.id}</td>
    </tr>`,
    )
    .join("");
}
