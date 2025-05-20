local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local asset_patterns = {
  --["^https?://([^/]*akamaized%.net/.+)$"]="asset",
  --["^https?://(av%.rferl%.org/.+)$"]="asset",
  ["^https?://(gdb%.rferl%.org/.+)$"]="asset",
  --["^https?://(media%.rferl%.org/.+)$"]="asset",
  ["^https?://(ssc%.[^/]*/.+)$"]="asset",
  ["^https?://(tags%.[^/]*/.+)$"]="asset",
  ["^https?://projects%.rferl%.org/([^/]*)"]="project"
}
local item_patterns = {
  ["^https?://www%.rferl%.org/a/([0-9]+)%.html$"]="article"
}
for k, v in pairs(asset_patterns) do
  item_patterns[k] = v
end
local rferlsites = {}
local rferlsites_file = io.open("rferlsites.txt", "r")
for site in rferlsites_file:lines() do
  rferlsites[site] = true
end
rferlsites_file:close()

check_rferlsite = function(site)
  if site == nil then
    return false
  end
  local a, b = string.match(site, "^([^%.]+)%.(.+)$")
  if a == "www" or a == "m" or a == "www1" then
    site = b
  end

  if rferlsites[site] then
    return true
  end
  return false
end

is_supported_media = function(d, must_match)
  local any_match = false
  if type(d) == "string" then
    d = {d}
  end
  for k, v in pairs(d) do
    if type(v) == "string"
      and string.match(v, "^https?://") then
      if string.match(v, "^https?://[^/]*akamaized%.net/.")
        or string.match(v, "^https?://av%.rferl%.org/.")
        or string.match(v, "^https?://rfe%-video%.rferl%.org/.")
        or string.match(v, "^https?://rfe%-video%-hls%.rferl%.org/.")
        or string.match(v, "^https?://rfe%-audio%.rferl%.org/.")
        or string.match(v, "^https?://rfe%-audio%-hls%.rferl%.org/.") then
        any_match = true
      elseif must_match then
        error("Unexpected media URL " .. v)
      end
    elseif type(v) == "table"
      and is_supported_media(v, must_match) then
      any_match = true
    end
  end
  return any_match
end

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item]
    and not string.match(item, ":$") then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_patterns) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {["any_200"]=false}
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      if item_name
        and not context["any_200"] then
        abort_item()
      end
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if is_supported_media(url, false) then
    if not parenturl
      or (parenturl and not string.match(parenturl, "^https?://[^/]+/amp/")) then
      return true
    end
    return false
  end

  if not parenturl
    or not string.match(parenturl, "^https?://[^/]*dwcdn%.net/") then
    local a = string.match(url, "^https?://datawrapper%.dwcdn%.net/([0-9a-zA-Z]+)/")
    if a then
      ids[string.lower(a)] = true
    end
  end

  if string.match(url, "^https?://docs%.rferl%.org/.") then
    return true
  end

  if string.match(url, "^https?://facebook%.com/share")
    or string.match(url, "^https?://twitter%.com/share")
    or string.match(url, "^https?://web%.whatsapp%.com/send")
    or string.match(url, "^https?://telegram%.me/share")
    or string.match(url, "^https?://line%.me/R/")
    or string.match(url, "^https?://timeline%.line%.me/social%-plugin/share")
    or string.match(url, "^https?://$")
    or string.match(url, "^https?://a/$")
    or string.match(url, "^https?://ssc%.")
    or string.match(url, "^https?://[^/]*disqus%.com/")
    or string.match(url, "^https?://gdb%.rferl%.org/Tealium%.aspx%?")
    or string.match(url, "^https?://test%-ltr%.rferl%.eu/")
    or string.match(url, "^https?://morigin%.")
    or string.match(url, "^https?://origin%-test%.")
    or string.match(url, "^https?://im%-media%.voltron%.rferl%.org/")
    or string.match(url, "^https?://[^/]*@")
    or string.match(url, "^https?://media%.voltron%.rferl%.org/") then
    return false
  end

  local skip = false
  for pattern, type_ in pairs(asset_patterns) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        skip = true
      end
    end
  end
  if skip then
    return false
  end

  if not check_rferlsite(string.match(url, "^https?://([^/]+)"))
    and not string.match(url, "^https?://[^/]*dwcdn%.net/")
    and not string.match(url, "^https?://[^/]*datawrapper%.de/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([0-9]+)",
    "([0-9a-zA-Z]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  local checked = {}

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if checked[newurl] then
      return nil
    end
    checked[newurl] = true
    if string.match(newurl, "^%s") then
      newurl = string.match(newurl, "^%s*(.-)%s*$")
    end
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = string.match(newurl, "^%s*(.-)%s*$")
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    if string.match(newurl, "%s") then
      for newurl2 in string.gmatch(newurl, "([^%s]+)") do
        check(newurl2)
      end
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if string.match(url_, "^https?://[^/]+https?://") then
      local domain1, domain2 = string.match(url_, "^https?://([^/]+)https?://([^/]+)")
      if domain1 == domain2 then
        url_ = string.match(url_, "^https?://[^/]+(https?://.+)$")
      end
    end
    if string.match(url_, "^https?://gdb%.rferl%.org/[^/]+$")
      and not string.match(url_, "^https?://gdb%.rferl%.org/Tealium%.aspx%?") then
      if not string.match(url_, "_s%....?.?$") then
        local a, b = string.match(url_, "^(https?://.+)(%....?.?)$")
        local url2 = a .. "_s" .. b
        if not addedtolist[url2] then
          check(url2)
        end
      end
      if string.match(url_, "_q[0-9]+_") then
        local url2 = string.gsub(url_, "_q[0-9]+_", "_")
        check(url2)
      end
      if string.match(url_, "/[^/_]+_[^/]+$") then
        local a, b = string.match(url_, "^(.+/[^_]+)_[^%.]+(%.[^%./_]+)$")
        check(a .. b)
      end
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
--print('QUEUING', url_)
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    if string.match(newurl, "^%s") then
      newurl = string.match(newurl, "^%s*(.-)%s*$")
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  if item_type == "article" then
    for site, _ in pairs(rferlsites) do
      extra = ""
      if not string.match(site, "%..+%.") then
        extra = "www."
      end
      check("https://" .. extra .. site .. "/a/" .. item_value .. ".html")
    end
  end

  if string.match(url, "^https?://www%.rferl%.org/") then
    local path = string.match(url, "^https?://[^/]+(/.*)$")
    check("https://www.rferl.eu" .. path)
  end

  if allowed(url)
    and status_code < 300
    and (
      (
        item_type ~= "asset"
        and not is_supported_media(url, false)
      )
      or string.match(url, "%.m3u8")
    )
    and not string.match(url, "^https?://docs%.rferl%.org/") then
    html = read_file(file)
    html = string.gsub(html, "(<video [^>]+>)", function (s)
      local sdkid = string.match(s, 'data%-sdkid="([0-9]+)"')
      if sdkid then
        if not ids[sdkid] then
          return ""
        end
      end
      return s
    end)
    html = string.gsub(html, '(<li class="subitem">.-</li>)', function (s)
      local video_id = string.match(s, "FireAnalyticsTagEventOnDownload%([^,]+,[^,]+,%s+([0-9]+),")
      if video_id
        and video_id ~= item_value then
        return ""
      end
      return s
    end)
    html = string.gsub(html, '<a class="c%-mmp__fallback%-link" href="[^"]+"%s*>', "")
    if string.match(url, "^https?://datawrapper%.dwcdn%.net/[^/]+/[0-9]+/$") then
      local version = string.match(url, "([0-9]+)/$")
      for i=1,tonumber(version) do
        check(urlparse.absolute(url, "../" .. tostring(i) .. "/"))
      end
    end
    if string.match(url, "^https?://datawrapper%.dwcdn%.net/.+/$") then
      local data = string.match(html, "window%.__DW_SVELTE_PROPS__%s*=%s*JSON%.parse%((\".-\")%);\n")
      if data then
        local json = cjson.decode(cjson.decode(data))
        html = html .. flatten_json(json)
        for _, d in pairs(json["assets"]) do
          check(urlparse.absolute(url, d["url"]))
        end
      end
    end
    for data in string.gmatch(html, "(<video [^>]+)") do
      local src = string.match(data, 'src="([^"]+)"')
      if src then
        if not is_supported_media(src, true) then
          error("Unsupported media stream.")
        end
      else
        local data_sources = string.match(data, 'data%-sources="([^"]+)"')
        if not data_sources then
          error("Could not find video sources.")
        end
        data_sources = cjson.decode(html_entities.decode(data_sources))
        if not is_supported_media(data_sources, true) then
          error("Unsupported media stream.")
        end
      end
    end
    local meta_tags = {}
    for data in string.gmatch(html, "(<meta [^>]+)") do
      local content = string.match(data, 'content="([^"]+)"')
      if content then
        for _, key in pairs({"name", "property"}) do
          local value = string.match(data, key .. '="([^"]+)"')
          if value then
            meta_tags[key .. "_" .. value] = content
          end
        end
      end
    end
    if meta_tags["name_twitter:player:stream"]
      or meta_tags["name_twitter:player:stream:content_type"] then
      if not is_supported_media(meta_tags["name_twitter:player:stream"], true) then
        error("Unsupported media stream.")
      end
    end
    local img_enhancer = string.match(html, "imgEnhancerBreakpoints%s*=%s*%[([0-9,%s]+)%];")
    if img_enhancer then
      local sizes = {}
      local max_size = 0
      for num in string.gmatch(img_enhancer, "([0-9]+)") do
        sizes[num] = true
        num = tonumber(num)
        if num > max_size then
          max_size = num
        end
      end
      if max_size == 0 then
        error("Maximum breakpoint size found is 0.")
      end
      context["sizes"] = sizes
      context["max_size"] = max_size
      if not meta_tags["property_og:image"] then
        error("Could not find main image.")
      end
      if string.match(meta_tags["property_og:image"], "_w1200_") then
        local a, b = string.match(meta_tags["property_og:image"], "^(.-_w)1200(_.+)$")
        for size, _ in pairs(sizes) do
          check(a .. size .. b)
        end
        a, b = string.match(meta_tags["property_og:image"], "^(https?://.-)_[^/%.]+(%.[a-zA-Z0-9]+)$")
        check(a .. b)
      end
    end
    if context["sizes"] then
      for img_data in string.gmatch(html, "(<img%s[^>]+>)") do
        local src = string.match(img_data, "src=\"([^\"]+)\"")
        if src and string.match(src, "^https?://gdb%.rferl%.org/.")
          and string.match(src, "_w[0-9]+[_%.]") then
          local a, b = string.match(src, "^(https?://.+_w)[0-9]+([_%.].+)$")
          local b, c = string.match(b, "^(.*)(%.[^%.]+)$")
          for size, _ in pairs(context["sizes"]) do
            check(a .. size .. b .. c)
          end
          check(a .. tostring(context["max_size"]) .. "_n" .. b .. "_s" .. c)
        end
      end
    end
    if string.match(url, "%.m3u8") then
      for line in string.gmatch(html, "([^\n]+)") do
        if string.len(string.match(line, "([^%s]+)")) > 0 then
          local newurl = urlparse.absolute(url, line)
          ids[newurl] = true
          check(newurl)
        end
      end
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "%(([^%)]+)%)") do
      check(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] == 200 then
    context["any_200"] = true
  end
  if http_stat["statcode"] == 301
    and is_supported_media(url["url"], false) then
    ids[urlparse.absolute(url["url"], http_stat["newloc"])] = true
  end
  if http_stat["statcode"] ~= 200
    and (
      http_stat["statcode"] ~= 301
      or (
        not (
          string.match(url["url"], "^https://[^/]+/a/[0-9]+%.html")
          and check_rferlsite(string.match(url["url"], "^https?://([^/]+)"))
        )
        and not ids[urlparse.absolute(url["url"], http_stat["newloc"])]
        and not string.match(url["url"], "^https?://www%.rferl%.eu/")
      )
    )
    and (
      http_stat["statcode"] ~= 404
      or not (
        string.match(url["url"], "^https://[^/]+/a/[0-9]+%.html")
        and check_rferlsite(string.match(url["url"], "^https?://([^/]+)"))
      )
    )
    and (
      http_stat["statcode"] ~= 302
      or not string.match(url["url"], "^https?://www%.datawrapper%.de/_/[0-9a-zA-Z]+$")
    )
    --[[and (
      http_stat["statcode"] ~= 302
      or (
        not string.match(url["url"], "^https?://www%.voacambodia%.com/")
        and not string.match(url["url"], "^https?://www%.dandalinvoa%.com/")
      )
    )]] then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300
    and not string.match(url["url"], "^https?://[^/]*dwcdn%.net/") then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 3
    if status_code == 403 then
      maxtries = 10
    end
    if status_code == 404 then
      tries = maxtries + 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["rferl-2c3vlcuoz7nwduvs"] = discovered_items,
    ["urls-ilvzm4k5wf6meg6n"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if not context["any_200"] then
    abort_item()
  end
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    return wget.exits.IO_FAIL
    --abort_item()
  end
  return exit_status
end


