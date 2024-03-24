(function () {
    console.log("search-pbi")

    downloadSearchIndex();
    downloadAndPopulateCorpus();

    async function downloadSearchIndex() {
        const response = await fetch("/search_index.json");
        const search = await response.json();
        window.index = lunr.Index.load(search);
    }

    async function downloadAndPopulateCorpus() {
        const response = await fetch("/corpus.json");
        const corpus = await response.json();
        window.site_contents = [];

        corpus.forEach(element => {
            var doc = {
                'id': element.id,
                'content': element.content,
                'name': element.name,
                'category': element.category,
                'url': element.url,

            };
            window.site_contents.push(doc);
        });
    }

    function renderSearchResults(results) {

        const resultsArea = document.getElementById("search-results");
        resultsArea.innerHTML = "";

        if (!results || !results.length) {
            resultsArea.innerHTML = "<li>No results found.</li>"
            return;
        }

        console.log("Got results")
        results.forEach(result => {
            var contentItem = window.site_contents.filter(contentItem => contentItem.id === result.ref);
            const resultElement = buildSearchResult(contentItem[0]);
            resultsArea.append(resultElement);
        });
    }

    function buildSearchResult(contentItem) {
        console.log(contentItem)
        var li = document.createElement('li'),
            article = document.createElement('article'),
            header = document.createElement('header'),
            section = document.createElement('section'),
            h2 = document.createElement('h2'),
            a = document.createElement('a'),
            p1 = document.createElement('p')

        a.dataset.field = 'url';
        a.href += contentItem.url;
        a.textContent = contentItem.name;

        p1.dataset.field = 'content';
        p1.textContent = contentItem.content;
        p1.style.textOverflow = 'ellipsis';
        p1.style.overflow = 'hidden';
        p1.style.whiteSpace = 'nowrap';

        li.appendChild(article);
        article.appendChild(header);
        article.appendChild(section);
        header.appendChild(h2);
        h2.appendChild(a);
        section.appendChild(p1);

        return li;
    }

    const form = document.getElementById('search-form');
    form.addEventListener('submit', function (event) {
        event.preventDefault();

        // Your code to handle the form submission goes here
        // For example, you can access form data using the FormData API:
        const formData = new FormData(form);
        const searchTerm = formData.get("search-query")
        var results = window.index.search(searchTerm)
        renderSearchResults(results)
    });
})();
