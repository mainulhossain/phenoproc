function branchListItem(base, o) {
	//li with children ul
	base.find('li').addBack('li').has("ul").each(function() {
		var branch = $(this);
		if (branch.hasClass('branch'))
			return;
		branch.prepend("<i class='indicator glyphicon "	+ o.closedClass + "'></i>");
		branch.addClass('branch');
		branch.on('click', function(e) {
			if (this == e.target) {
				var icon = $(this).children('i:first');
				icon.toggleClass(o.openedClass + " " + o.closedClass);
				$(this).children().children().toggle();
			}
		})
		branch.children().children().toggle();
	});
}

function branchListItemPlusMinusIcon(branch) {
	
	branchListItem(branch, {
				'openedClass': 'glyphicon-minus-sign',
				'closedClass': 'glyphicon-plus-sign'
		});
}

function attachEvents(base) {
	//fire event from the dynamically added icon
	base.find('.branch .indicator').each(function() {
		$(this).on('click', function() {
			$(this).closest('li').click();
		});
	});

	base.on("click", "li > a", function(e) {
		base.find("li > a").removeClass('selected');
		$(this).attr('class', 'selected');
	});

	//fire event to open branch if the li contains an anchor instead of text
	base.find('.branch>a').each(function() {
		$(this).on('click', function(e) {
			$(this).closest('li').click();
			e.preventDefault();
		});
	});
	//fire event to open branch if the li contains a button instead of text
	base.find('.branch>button').each(function() {
		$(this).on('click', function(e) {
			$(this).closest('li').click();
			e.preventDefault();
		});
	});
}

$.fn.extend({
	treed : function(o) {
		
		var arg = {
				'openedClass': 'glyphicon-minus-sign',
				'closedClass': 'glyphicon-plus-sign'
		};
		
		if (typeof o != 'undefined') {
			if (typeof o.openedClass != 'undefined') {
				arg.openedClass = o.openedClass;
			}
			if (typeof o.closedClass != 'undefined') {
				arg.closedClass = o.closedClass;
			}
		};
		
		//initialize each of the top levels
		var tree = $(this);
		tree.addClass("tree");
		branchListItem(tree, arg); 
				
		
		// attach events to whole
		attachEvents(tree);
	}
});