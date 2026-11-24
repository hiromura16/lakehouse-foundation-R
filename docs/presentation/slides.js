"use strict";

// スライド本体はHTMLに置く。ここでは目次、表示位置、履歴だけを管理する。
const pages = Array.from(document.querySelectorAll("#deck > .slide"));
const controls = document.querySelector(".controls");
const previous = document.querySelector("#previous");
const next = document.querySelector("#next");
const position = document.querySelector("#position");
const progress = document.querySelector("#progress");
const announcer = document.querySelector("#announcer");
const outline = document.querySelector("#outline");
const outlineLinks = document.querySelector("#outline-links");
const pageTitles = pages.map((page) => page.querySelector("h1, h2"));
const indexes = new Map(pages.map((page, index) => [`#${page.id}`, index]));
let selected = 0;

function buildOutline() {
  // デッキの順番と章名を使い、目次の各項目をHTMLの見出しと一致させる。
  const lists = new Map();
  return pages.map((page, index) => {
    const chapter = page.dataset.chapter;
    if (!lists.has(chapter)) {
      const title = document.createElement("h3");
      const list = document.createElement("ol");
      title.textContent = chapter;
      outlineLinks.append(title, list);
      lists.set(chapter, list);
    }
    const item = document.createElement("li");
    const link = document.createElement("a");
    const number = document.createElement("span");
    number.className = "outline-number";
    number.textContent = String(index + 1).padStart(2, "0");
    link.href = `#${page.id}`;
    link.append(number, document.createTextNode(pageTitles[index].innerText.replace(/\s+/g, " ")));
    item.append(link);
    lists.get(chapter).append(item);
    return link;
  });
}

const outlineItems = buildOutline();

function showSlide(index) {
  // 読み上げとブラウザー印刷が参照する見出しは、表示切替で作り直さない。
  selected = index;
  for (const [pageIndex, page] of pages.entries()) {
    const isSelected = pageIndex === selected;
    page.hidden = !isSelected;
    if (isSelected) outlineItems[pageIndex].setAttribute("aria-current", "page");
    else outlineItems[pageIndex].removeAttribute("aria-current");
  }
  previous.disabled = selected === 0;
  next.disabled = selected === pages.length - 1;
  const pageNumber = selected + 1;
  position.textContent = `${pageNumber} / ${pages.length}`;
  progress.value = pageNumber;
  const title = pageTitles[selected].innerText.replace(/\s+/g, " ");
  announcer.textContent = `${position.textContent}　${title}`;
  document.title = `${title}｜Foundation R`;
  window.scrollTo(0, 0);
}

function navigate(index, focusHeading = false) {
  if (index < 0 || index >= pages.length) return;
  const target = `#${pages[index].id}`;
  if (target !== location.hash) history.pushState(null, "", target);
  if (index !== selected) showSlide(index);
  if (focusHeading) pageTitles[index].focus({ preventScroll: true });
}

for (const title of pageTitles) title.tabIndex = -1;
progress.max = pages.length;
controls.hidden = false;
document.documentElement.classList.add("enhanced");
showSlide(indexes.get(location.hash) ?? 0);

previous.addEventListener("click", () => navigate(selected - 1));
next.addEventListener("click", () => navigate(selected + 1));
document.querySelector("#print").addEventListener("click", () => window.print());
window.addEventListener("hashchange", () => showSlide(indexes.get(location.hash) ?? 0));
window.addEventListener("popstate", () => showSlide(indexes.get(location.hash) ?? 0));

document.querySelector("#outline-toggle").addEventListener("click", () => {
  outline.showModal();
  outlineItems[selected].focus({ preventScroll: true });
  outlineItems[selected].scrollIntoView({ block: "nearest" });
});

document.addEventListener("click", (event) => {
  // 章へのリンクはアドレスバーの位置と表示を揃える。別タブ操作は通常どおり。
  if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  const link = event.target.closest("a[href^='#']");
  const index = indexes.get(link?.hash);
  if (index === undefined) return;
  event.preventDefault();
  if (outline.open) outline.close();
  navigate(index, true);
});

document.addEventListener("keydown", (event) => {
  // 目次内の移動やブラウザーのショートカットを優先する。
  if (outline.open || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
  if (event.target.closest("input, select, textarea, [contenteditable]")) return;
  const shortcuts = {
    ArrowRight: selected + 1,
    PageDown: selected + 1,
    ArrowLeft: selected - 1,
    PageUp: selected - 1,
    Home: 0,
    End: pages.length - 1,
  };
  if (!Object.hasOwn(shortcuts, event.key)) return;
  event.preventDefault();
  navigate(shortcuts[event.key]);
});
