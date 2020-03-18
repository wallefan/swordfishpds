(async function(modNames){
    async function fetchToDoc(){
        return await (fetch.apply(this, arguments).then(response=>response.text()).then(text=>new DOMParser().parseFromString(text, 'text/html')));
    }
    let mods = [];
    for(let modName of modNames){
        let modListing = await fetchToDoc(`https://www.curseforge.com/minecraft/mc-mods/${modName}/files/all?filter-game-version=1738749986:628`);
        let targetLink = modListing.querySelectorAll('a[data-action=file-link]')[0];
        let linkParts = /\/minecraft\/mc-mods\/(?:.*?)\/files\/(\d*?)$/.exec(targetLink.href);
        let modID = linkParts[1];
        let downloadDetailsPage = await fetchToDoc(targetLink);
        let downloadName = [...downloadDetailsPage.getElementsByTagName('span')].filter(x=>x.innerText.trim()=='Filename')[0].nextElementSibling.innerText;
        mods += `MOD,${modID},${downloadName}\n`;
    }
    console.log(mods);
})(['example-mod']);